import logging
from dotenv import load_dotenv
import lightgbm as lgb
import os
from concurrent import futures
import grpc
import numpy as np
from omegaconf import DictConfig
import pandas as pd
import hydra
from utils_prediction import build_flight_df
import priceest.prices_pb2_grpc as prices_pb2_grpc
from priceest.prices_pb2 import EstimatePriceRequest, EstimatePriceResponse
from minio_client import MinioClient
from minio.error import S3Error


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


@hydra.main(version_base="1.3", config_path="../configs/predict", config_name="config")
def main(cfg: DictConfig):
    logging.basicConfig()
    load_dotenv()
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_BUCKET_NAME_MODEL = os.getenv("MINIO_BUCKET_NAME_MODEL")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

    model_path = cfg.get("model_path")
    if len(os.listdir(model_path)) != 0:
        print("Model found")
        latest = model_path + max(
            [f for f in os.listdir(model_path)], key=os.path.getctime
        )
        model = lgb.Booster(model_file=latest)
        serve(model)
    else:
        print("Model not found, trying to download from MinIO...")
        client = MinioClient(
            MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
        )
        file = client.download_file(
            dest_dir=model_path,
            bucket_name=MINIO_BUCKET_NAME_MODEL,
            latest=True,
        )
        if not file:
            print("No model found in MinIO bucket, exiting...")
        else:
            model = lgb.Booster(model_file=model_path + file.object_name)
            serve(model)


if __name__ == "__main__":
    main()
