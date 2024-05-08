import logging
import lightgbm as lgb
import os
from concurrent import futures
import grpc
import numpy as np
import pandas as pd
from utils import build_flight_df
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


if __name__ == "__main__":
    try:
        logging.basicConfig()
        MODEL_PATH = os.getenv("MODEL_PATH")
        MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
        MINIO_BUCKET_NAME_MODEL = os.getenv("MINIO_BUCKET_NAME_MODEL")
        MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
        MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

        if MODEL_PATH and os.path.exists(MODEL_PATH):
            client = MinioClient(
                MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
            )
            file = client.download_file(
                dest_dir=MODEL_PATH,
                bucket_name=MINIO_BUCKET_NAME_MODEL,
                latest=True,
            )
            if not file:
                print("No model found")
            else:
                model = lgb.Booster(model_file=MODEL_PATH + file.object_name)
                serve(model)
        else:
            print("Model directory not found")

    except S3Error as exc:
        print("Error occurred in MinIO", exc)
