import logging
import lightgbm as lgb
import os, rootutils
from concurrent import futures
import grpc

rootutils.setup_root(__file__, indicator=".project-root", pythonpath=True)

import src.protos.priceest.prices_pb2_grpc as prices_pb2_grpc
import src.protos.priceest.prices_pb2 as prices_pb2


class PriceEstimation(prices_pb2_grpc.PriceEstimationServicer):
    def __init__(self, model: lgb.Booster):
        super().__init__()
        self.model = model

    def EstimatePrice(self, request, context):
        input = ""
        print("REQ: ", request)
        # price = model.predict(input)
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
    logging.basicConfig()
    MODEL_PATH = os.getenv("MODEL_PATH")
    MODEL_PATH = "out/"
    if MODEL_PATH and os.path.exists(MODEL_PATH):
        all_files = [MODEL_PATH + f for f in os.listdir(MODEL_PATH)]
        latest = max(all_files, key=os.path.getctime)
        model = lgb.Booster(model_file=latest)
        serve(model)
    else:
        print("Model directory not found")
