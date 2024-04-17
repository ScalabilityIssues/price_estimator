from datetime import datetime
import time
from typing import Any, Dict
import lightgbm as lgb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import hydra, os

from omegaconf import DictConfig, OmegaConf
from minio_client import MinioClient
from minio.error import S3Error
from dotenv import load_dotenv
from utils import extract_time_features, rmse


def get_df_from_csv(data_dir: str, file_name: str, date_format: str):
    df = pd.read_csv(data_dir + file_name, sep=";", header=0)
    df["date"] = pd.to_datetime(df["date"], format=date_format)
    df = df.set_index("date")
    df["source"] = df["source"].astype("category")
    df["destination"] = df["destination"].astype("category")

    df["start_time"] = pd.to_datetime(df["start_time"], format="%H:%M%z", utc=True)
    df["end_time"] = pd.to_datetime(df["end_time"], format="%H:%M%z", utc=True)

    df = extract_time_features(df)
    # Drop redundant or useless columns
    df = df.drop(["start_time", "end_time", "currency"], axis=1)
    df["price"] = df["price"].astype("float32")
    return df


def train(df: pd.DataFrame, params: Any):
    split_date = str(df.index[int(len(df) * 0.8)].date())
    train = df.loc[:split_date]
    test = df.loc[split_date:]

    # Split data into features and target
    X_train, y_train = train.drop("price", axis=1), train["price"]
    X_test, y_test = test.drop("price", axis=1), test["price"]

    lgb_train = lgb.Dataset(X_train, y_train)
    lgb_eval = lgb.Dataset(X_test, y_test, reference=lgb_train)

    # specify your configurations as a dict

    print("Starting training...")
    model = lgb.train(
        params,
        lgb_train,
        num_boost_round=20,
        valid_sets=[lgb_eval],
        callbacks=[lgb.early_stopping(stopping_rounds=5)],
        categorical_feature="auto",
    )
    """ ax = lgb.plot_importance(gbm, max_num_features=10)
    plt.show() """

    print("Starting predicting...")
    # plt.savefig("feature_importance.png")
    y_pred = model.predict(X_test, num_iteration=model.best_iteration)
    score = rmse(y_pred, y_test)
    print(f"RMSE Score on Test set: {score:0.3f}")
    return model


@hydra.main(version_base=None, config_path="../train_configs", config_name="config")
def main(cfg: DictConfig):
    load_dotenv()
    # MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    # MINIO_BUCKET_NAME_TRAINING = os.getenv("MINIO_BUCKET_NAME_TRAINING")
    # MINIO_BUCKET_NAME_MODEL = os.getenv("MINIO_BUCKET_NAME_MODEL")
    # MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    # MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

    MINIO_ENDPOINT = "localhost:9000"
    MINIO_BUCKET_NAME_TRAINING = "test-bucket"
    MINIO_BUCKET_NAME_MODEL = "model-bucket"
    MINIO_ACCESS_KEY = "root"
    MINIO_SECRET_KEY = "root1234"

    train_data_dir = cfg.get("data_dir")
    model_out_dir = cfg.get("model_out_dir")
    date_format = cfg.get("date_format")
    train_params = OmegaConf.to_object(cfg.get("train_params"))

    client = MinioClient(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    train_file_obj = client.download_file(
        dest_dir=train_data_dir, bucket_name=MINIO_BUCKET_NAME_TRAINING, latest=True
    )

    if not train_file_obj:
        print("No file found")
        return

    df = get_df_from_csv(
        data_dir=train_data_dir,
        file_name=train_file_obj.object_name,
        date_format=date_format,
    )

    model: lgb.Booster = train(df, train_params)
    model_name = "model" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".txt"
    model.save_model(model_out_dir + model_name)

    client.upload_file(
        bucket_name=MINIO_BUCKET_NAME_MODEL,
        source_dir=model_out_dir,
        file_name=model_name,
        latest=False,
        content_type="application/txt",
    )


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("Error occurred in MinIO", exc)
