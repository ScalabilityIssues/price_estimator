from datetime import datetime
from time import ctime
from typing import Any
from dotenv import load_dotenv
import lightgbm as lgb
import pandas as pd
import hydra, os

from omegaconf import DictConfig, OmegaConf
from minio import Minio
from progress import Progress
from utils_predict import rmse, build_flight_df
from functools import partial
import pika


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

    print("Starting training...")
    model = lgb.train(
        train_params,
        lgb_train,
        num_boost_round=20,
        valid_sets=[lgb_eval],
        callbacks=[lgb.early_stopping(stopping_rounds=5)],
        categorical_feature="auto",
    )

    print("Starting predicting...")
    y_pred = model.predict(X_test, num_iteration=model.best_iteration)
    score = rmse(y_pred, y_test)
    print(f"RMSE Score on Test set: {score:0.3f}")

    return model


def consume_callback(ch, method, properties, body, args):
    train_data_dir = args.get("data_dir")
    model_out_dir = args.get("model_out_dir")
    date_format = args.get("date_format")
    train_params = OmegaConf.to_object(args.get("train_params"))

    obj_name_train = body["Records"]["s3"]["object"]["key"]
    bucket_name_train = body["Records"]["s3"]["bucket"]["name"]
    minio_client = args.get("minio_client")
    bucket_name_model = args.get("bucket_name_model")

    print(f" [*] Received message for {obj_name_train} in bucket {bucket_name_train}")

    result = minio_client.fget_object(
        bucket_name=bucket_name_train,
        object_name=obj_name_train,
        file_path=train_data_dir + obj_name_train,
    )
    if result is None:
        print("[*] Error downloading the file")
        return

    print("[*] File downloaded successfully")
    model: lgb.Booster = train(
        train_data_dir + obj_name_train, train_params, date_format
    )
    model_name = "model_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".txt"
    model.save_model(model_out_dir + model_name)
    print(f"[*] Model saved in {model_out_dir + model_name}")

    result = minio_client.fput_object(
        bucket_name=bucket_name_model,
        object_name=model_name,
        file_path=model_out_dir + model_name,
        progress=Progress(),
        content_type="application/txt",
        metadata={"creation-date": ctime(os.path.getctime(model_out_dir + model_name))},
    )
    print(f"[*] Object {result.object_name} uploaded to MinIO bucket")


# Train the model and upload it to MinIO, if a message from RabbitMQ arrives (scraping).
# If the upload fails, write a log file to allow the resuming of the process later.
@hydra.main(version_base=None, config_path="../configs/train", config_name="config")
def main(cfg: DictConfig):
    load_dotenv()
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_BUCKET_NAME_TRAINING = os.getenv("MINIO_BUCKET_NAME_TRAINING")
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
    if not minio_client.bucket_exists(
        MINIO_BUCKET_NAME_TRAINING
    ) or not minio_client.bucket_exists(MINIO_BUCKET_NAME_MODEL):
        raise Exception(
            f"Bucket {MINIO_BUCKET_NAME_TRAINING} or bucket {MINIO_BUCKET_NAME_MODEL} do not exist"
        )

    connection_rabbitmq = pika.BlockingConnection(
        pika.ConnectionParameters(host="rabbitmq")
    )
    channel_rabbitmq = connection_rabbitmq.channel()

    if cfg.get("force_training"):
        cfg["minio_client"] = minio_client
        cfg["bucket_name_model"] = MINIO_BUCKET_NAME_MODEL
        channel_rabbitmq.basic_consume(
            queue="ml-data",
            on_message_callback=partial(consume_callback, args=cfg),
            auto_ack=True,
        )
        print(" [*] Waiting for messages. To exit press CTRL+C")
        channel_rabbitmq.start_consuming()
    else:
        print("Training not forced, skipping...")
        connection_rabbitmq.close()


if __name__ == "__main__":
    main()
