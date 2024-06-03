from functools import partial
import logging
from math import modf
import time
import threading
from dotenv import load_dotenv
import lightgbm as lgb
import os
import json
import traceback as tb
from concurrent import futures
import grpc
import numpy as np
from omegaconf import DictConfig
import pandas as pd
import hydra
import pika
from utils_predict import build_flight_df
from minio import Minio
import priceest.prices_pb2_grpc as prices_pb2_grpc
from priceest.prices_pb2 import EstimatePriceRequest, EstimatePriceResponse

log = logging.getLogger(__name__)


class PriceEstimation(prices_pb2_grpc.PriceEstimationServicer):
    def __init__(self, model: lgb.Booster | None):
        super().__init__()
        self.model = model

    def update_model(self, new_model: lgb.Booster | None):
        self.model = new_model

    def EstimatePrice(
        self, request: EstimatePriceRequest, context: grpc.ServicerContext
    ) -> EstimatePriceResponse:

        if self.model is None:
            raise context.abort(grpc.StatusCode.UNAVAILABLE, "Model not found")

        source = request.flight.source
        destination = request.flight.destination
        departure_time = request.flight.departure_time.ToDatetime()
        arrival_time = request.flight.arrival_time.ToDatetime()

        date = departure_time.strftime("%Y-%m-%d")
        start_time = departure_time.strftime("%H:%M")
        end_time = arrival_time.strftime("%H:%M")

        flight_detail = {
            "date": [date],
            "source": [source],
            "destination": [destination],
            "start_time": [start_time],
            "end_time": [end_time],
        }

        df = build_flight_df(pd.DataFrame(flight_detail), hour_format="%H:%M")
        price = self.model.predict(df)

        response = EstimatePriceResponse()
        response.price.currency_code = "USD"

        fractional, units = modf(price[0])
        response.price.units, response.price.nanos = int(units), int(fractional * 1e9)

        return response


class ModelStore:
    def __init__(self, minio_client, minio_bucket_name_model):
        self.minio_client = minio_client
        self.bucket_name = minio_bucket_name_model

    def try_get_latest_model(self):
        objects = self.minio_client.list_objects(self.bucket_name, include_user_meta=True)
        all_files = [
            (
                o.object_name,
                time.mktime(time.strptime(o.metadata["X-Amz-Meta-Creation-Date"], "%a %b %d %H:%M:%S %Y")),
            )
            for o in objects
        ]

        if len(all_files) == 0:
            log.info("No files found in the bucket")
            return None
        else:
            file_name = max(all_files, key=lambda x: x[1])[0]
            return self.get_model(file_name)

    def get_model(self, model_name: str):
        try:
            response = self.minio_client.get_object(
                bucket_name=self.bucket_name,
                object_name=model_name,
            )
            log.info(f"Model downloaded: {model_name}")
            model = lgb.Booster(model_str=response.read(decode_content=True).decode())
            return model
        finally:
            response.close()
            response.release_conn()


def grpc_serve(price_est_obj: PriceEstimation, model_store: ModelStore):
    model = model_store.try_get_latest_model()
    price_est_obj.update_model(model)

    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    prices_pb2_grpc.add_PriceEstimationServicer_to_server(price_est_obj, server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    log.info("GRPC server started, listening on " + port)
    server.wait_for_termination()


def rabbitmq_listen(args: dict):
    connection_rabbitmq = pika.BlockingConnection(
        pika.ConnectionParameters(host="rabbitmq")
    )
    channel_rabbitmq = connection_rabbitmq.channel()
    channel_rabbitmq.basic_consume(
        queue="ml-model",
        on_message_callback=partial(consume_callback, args=args),
        auto_ack=True,
    )
    log.info("Listening for new models from RabbitMQ")
    channel_rabbitmq.start_consuming()


def consume_callback(ch, method, properties, body, args):
    try:
        body = json.loads(body.decode().replace("'", '"'))

        price_est_obj: PriceEstimation = args.get("price_est_obj")
        model_store: ModelStore = args.get("model_store")

        obj_name_pred = body["Records"][0]["s3"]["object"]["key"]
        bucket_name_pred = body["Records"][0]["s3"]["bucket"]["name"]
        log.info(f" [*] Received message for {obj_name_pred} in bucket {bucket_name_pred}")

        model = model_store.get_model(obj_name_pred)
        price_est_obj.update_model(model)

    except Exception as e:
        tb.log.info_exc()


# Download the model from MinIO and start prediction server.
# If the model in not found in MinIO, wait for a message from RabbitMQ.
@hydra.main(version_base="1.3", config_path="../configs/predict", config_name="config")
def main(cfg: DictConfig):
    load_dotenv()
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_BUCKET_NAME_MODEL = os.getenv("MINIO_BUCKET_NAME_MODEL")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    secure_connection = cfg.get("secure_connection")

    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure_connection,
    )
    if not minio_client.bucket_exists(MINIO_BUCKET_NAME_MODEL):
        raise Exception(f"Bucket {MINIO_BUCKET_NAME_MODEL} do not exist")

    model_store = ModelStore(minio_client, MINIO_BUCKET_NAME_MODEL)

    price_est_obj = PriceEstimation(None)
    grpc_service_thread = threading.Thread(
        target=grpc_serve,
        kwargs={'price_est_obj': price_est_obj, 'model_store': model_store})
    rabbitmq_service_thread = threading.Thread(
        target=rabbitmq_listen,
        kwargs={'args': dict(cfg) | {'model_store': model_store, 'price_est_obj': price_est_obj}})

    log.info("Threads starting...")
    grpc_service_thread.start()
    rabbitmq_service_thread.start()

    grpc_service_thread.join()
    rabbitmq_service_thread.join()


if __name__ == "__main__":
    main()
