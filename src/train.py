import json
import logging
import os
import traceback as tb
from datetime import datetime
from functools import partial
from time import ctime
from typing import Any

import hydra
import lightgbm as lgb
import pandas as pd
import pika
from dotenv import load_dotenv
from minio import Minio
from omegaconf import DictConfig, OmegaConf

from progress import Progress
from utils_predict import build_flight_df, rmse

log = logging.getLogger(__name__)


def train(file_path, train_params: Any, date_format: str):
    df = pd.read_csv(file_path, sep=";", header=0)
    df = build_flight_df(df, date_format=date_format)

    # Drop redundant or useless columns
    df = df.drop(["currency"], axis=1)
    df["price"] = df["price"].astype("float32")

    split_date = str(df.index[int(len(df) * 0.8)].date())
    train = df.loc[:split_date]
    test = df.loc[split_date:]

    # Split data into features and target
    X_train, y_train = train.drop("price", axis=1), train["price"]
    X_test, y_test = test.drop("price", axis=1), test["price"]

    lgb_train = lgb.Dataset(X_train, y_train)
    lgb_eval = lgb.Dataset(X_test, y_test, reference=lgb_train)

    log.info("Starting training...")
    model = lgb.train(
        train_params,
        lgb_train,
        num_boost_round=20,
        valid_sets=[lgb_eval],
        callbacks=[lgb.early_stopping(stopping_rounds=5)],
        categorical_feature="auto",
    )

    log.info("Starting testing...")
    y_pred = model.predict(X_test, num_iteration=model.best_iteration)
    score = rmse(y_pred, y_test)
    log.info(f"RMSE Score on test set: {score:0.3f}")

    return model


def consume_callback(ch, method, properties, body, args):
    try:
        body = json.loads(body.decode().replace("'", '"'))
        train_data_dir = args.get("train_data_dir")
        model_out_dir = args.get("model_out_dir")
        date_format = args.get("date_format")
        train_params = OmegaConf.to_object(args.get("train_params"))

        obj_name_train = body["Records"][0]["s3"]["object"]["key"]
        bucket_name_train = body["Records"][0]["s3"]["bucket"]["name"]
        minio_client = args.get("minio_client")
        bucket_name_model = args.get("bucket_name_model")

        log.info(f" [*] Received message for {obj_name_train} in bucket {bucket_name_train}")

        result = minio_client.fget_object(
            bucket_name=bucket_name_train,
            object_name=obj_name_train,
            file_path=train_data_dir + obj_name_train,
        )
        if result is None:
            raise Exception("Error downloading the file")

        log.info("[*] File downloaded successfully")
        model: lgb.Booster = train(
            train_data_dir + obj_name_train, train_params, date_format
        )
        model_name = "model_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".txt"
        model.save_model(model_out_dir + model_name)
        log.info(f"[*] Model saved in {model_out_dir + model_name}")

        result = minio_client.fput_object(
            bucket_name=bucket_name_model,
            object_name=model_name,
            file_path=model_out_dir + model_name,
            progress=Progress(),
            content_type="application/txt",
            metadata={
                "creation-date": ctime(os.path.getctime(model_out_dir + model_name))
            },
        )
        log.info(f"[*] Object {result.object_name} uploaded to MinIO bucket")
    except Exception as e:
        tb.log.info_exc()


# Train the model and upload it to MinIO, if a message from RabbitMQ arrives (scraping).
# Run only if force_training is set to True in the config file.
@hydra.main(version_base=None, config_path="../configs/train", config_name="config")
def main(cfg: DictConfig):

    if cfg.get("force_training"):
        load_dotenv()

        minio_client = Minio(
            endpoint=cfg.minio.endpoint,
            access_key=cfg.minio.access_key,
            secret_key=cfg.minio.secret_key,
            secure=cfg.minio.secure_connection,
        )
        if not minio_client.bucket_exists(cfg.minio.bucket_name_model):
            raise Exception(f"Bucket {cfg.minio.bucket_name_model} do not exist")

        connection_rabbitmq = pika.BlockingConnection(
            pika.ConnectionParameters(host="rabbitmq")
        )
        channel_rabbitmq = connection_rabbitmq.channel()

        args = dict(cfg)
        args["minio_client"] = minio_client
        args["bucket_name_model"] = cfg.minio.bucket_name_model
        # log.info(args)

        channel_rabbitmq.basic_consume(
            queue="ml-data",
            on_message_callback=partial(consume_callback, args=args),
            auto_ack=True,
        )
        log.info(" [*] Waiting for messages. To exit press CTRL+C")
        channel_rabbitmq.start_consuming()
        connection_rabbitmq.close()
    else:
        log.warn("Training not forced, skipping...")


if __name__ == "__main__":
    main()
