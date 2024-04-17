import logging
from dotenv import load_dotenv
import lightgbm as lgb
import os
from concurrent import futures
import grpc


import priceest.prices_pb2_grpc as prices_pb2_grpc
from priceest.prices_pb2 import EstimatePriceRequest, EstimatePriceResponse
from minio_client import MinioClient
from minio.error import S3Error


class PriceEstimation(prices_pb2_grpc.PriceEstimationServicer):
    def __init__(self, model: lgb.Booster):
        super().__init__()
        self.model = model

    def EstimatePrice(self, request: EstimatePriceRequest, context) -> EstimatePriceResponse:
        input = ""
        print("REQ: ", request)
        price = self.model.predict(input)
        pass


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
        load_dotenv()
        # MODEL_PATH = os.getenv("MODEL_PATH")
        # MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
        # MINIO_BUCKET_NAME_MODEL = os.getenv("MINIO_BUCKET_NAME_MODEL")
        # MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
        # MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

        MINIO_ENDPOINT = "localhost:9000"
        MINIO_BUCKET_NAME_MODEL = "model-bucket"
        MINIO_ACCESS_KEY = "root"
        MINIO_SECRET_KEY = "root1234"
        MODEL_PATH = "out/"
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
