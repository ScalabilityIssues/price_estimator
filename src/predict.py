from functools import partial
import time
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


class PriceEstimation(prices_pb2_grpc.PriceEstimationServicer):
    def __init__(self, model: lgb.Booster):
        super().__init__()
        self.model = model

    def EstimatePrice(
        self, request: EstimatePriceRequest, context
    ) -> EstimatePriceResponse:

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
        decimal, integer = np.modf(price)
        response.price.units, response.price.nanos = (
            int(decimal[0]),
            int(integer[0]),
        )
        return response


def serve(model: lgb.Booster):
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    prices_pb2_grpc.add_PriceEstimationServicer_to_server(
        PriceEstimation(model), server
    )
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


def consume_callback(ch, method, properties, body, args):
    try:
        body = json.loads(body.decode().replace("'", '"'))
        model_path = args.get("model_dir")
        minio_client = args.get("minio_client")

        obj_name_pred = body["Records"][0]["s3"]["object"]["key"]
        bucket_name_pred = body["Records"][0]["s3"]["bucket"]["name"]
        print(f" [*] Received message for {obj_name_pred} in bucket {bucket_name_pred}")

        minio_client.fget_object(
            bucket_name=bucket_name_pred,
            object_name=obj_name_pred,
            file_path=model_path + obj_name_pred,
        )
        if not os.path.exists(model_path + obj_name_pred):
            raise Exception("Error downloading the file")
        ch.stop_consuming()
    except Exception as e:
        tb.print_exc()


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
    model_dir = cfg.get("model_dir")

    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure_connection,
    )
    if not minio_client.bucket_exists(MINIO_BUCKET_NAME_MODEL):
        raise Exception(f"Bucket {MINIO_BUCKET_NAME_MODEL} do not exist")

    default_model_name = cfg.get("default_model_name")
    if default_model_name is None:
        objects = minio_client.list_objects(
            MINIO_BUCKET_NAME_MODEL, include_user_meta=True
        )
        all_files = [
            (
                o.object_name,
                time.mktime(
                    time.strptime(
                        o.metadata["X-Amz-Meta-Creation-Date"], "%a %b %d %H:%M:%S %Y"
                    )
                ),
            )
            for o in objects
        ]
        if len(all_files) == 0:
            print("No files found in the bucket")
            connection_rabbitmq = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
            args = dict(cfg)
            channel_rabbitmq = connection_rabbitmq.channel()
            args["minio_client"] = minio_client
            channel_rabbitmq.basic_consume(
                queue="ml-model",
                on_message_callback=partial(consume_callback, args=args),
                auto_ack=True,
            )
            print(" [*] Waiting for messages. To exit press CTRL+C")
            channel_rabbitmq.start_consuming()
            connection_rabbitmq.close()

            if os.path.exists(model_dir):
                all_files = [
                    model_dir + f
                    for f in os.listdir(model_dir)
                    if not (model_dir + f).endswith(".gitkeep")
                ]
                latest_file = max(all_files, key=os.path.getctime)
                model = lgb.Booster(model_file=latest_file)
                serve(model)
            else:
                print("No model found")
        else:
            file_name = max(all_files, key=lambda x: x[1])[0]
            result = minio_client.fget_object(
                bucket_name=MINIO_BUCKET_NAME_MODEL,
                object_name=file_name,
                file_path=model_dir + file_name,
            )
            if result is None:
                print("[*] Error downloading the file")
                return
            model = lgb.Booster(model_file=model_dir + file_name)
            serve(model)
    else:
        result = minio_client.fget_object(
            bucket_name=MINIO_BUCKET_NAME_MODEL,
            object_name=default_model_name,
            file_path=model_dir + default_model_name,
        )
        if result is None:
            print(f'[*] Error downloading the default model "{default_model_name}"')
            return
        model = lgb.Booster(model_file=model_dir + default_model_name)
        serve(model)


if __name__ == "__main__":
    main()
