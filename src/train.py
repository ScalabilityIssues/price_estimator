import time
from typing import Any, Dict
from sklearn.metrics import mean_squared_error
import lightgbm as lgb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import hydra, os, rootutils

rootutils.setup_root(__file__, indicator=".project-root", pythonpath=True)
from omegaconf import DictConfig, OmegaConf, SCMode
from minio import Minio
from dotenv import load_dotenv

load_dotenv()


def get_minio_data(data_dir_out: str):
    # MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    # MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    # MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    # MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

    MINIO_ENDPOINT = "localhost:9000"
    MINIO_BUCKET_NAME = "test-bucket"
    MINIO_ACCESS_KEY = "root"
    MINIO_SECRET_KEY = "root1234"

    secure = True
    if "localhost" in MINIO_ENDPOINT or "127.0.0.1" in MINIO_ENDPOINT:
        secure = False
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )

    objects = client.list_objects(MINIO_BUCKET_NAME, include_user_meta=True)
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

    latest = max(all_files, key=lambda x: x[1])[0]
    file_path = data_dir_out + latest
    file = client.fget_object(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=latest,
        file_path=file_path,
    )
    return file_path


def get_df(file_path: str, date_format: str):
    # TODO: add the get from minio
    df = pd.read_csv(file_path, sep=";", header=0)
    df["date"] = pd.to_datetime(df["date"], format=date_format)
    df = df.set_index("date")

    df["source"] = df["source"].astype("category")
    df["destination"] = df["destination"].astype("category")

    df["start_time"] = pd.to_datetime(df["start_time"], format="%H:%M%z", utc=True)
    df["end_time"] = pd.to_datetime(df["end_time"], format="%H:%M%z", utc=True)

    df["duration"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 60
    df["duration"] = df["duration"].astype("int32")

    df["hour_start_time"] = df["start_time"].dt.hour
    df["hour_end_time"] = df["end_time"].dt.hour
    df["minutes_start_time"] = df["start_time"].dt.minute
    df["minutes_end_time"] = df["end_time"].dt.minute
    # Drop redundant or useless columns
    df = df.drop(["start_time", "end_time", "currency"], axis=1)

    # Features from date index Time series
    df["dayofweek"] = df.index.dayofweek
    df["month"] = df.index.month
    df["year"] = df.index.year
    df["dayofyear"] = df.index.dayofyear
    df["dayofmonth"] = df.index.day
    df["weekofyear"] = df.index.isocalendar().week
    df["weekofyear"] = df["weekofyear"].astype("int32")

    df["price"] = df["price"].astype("float32")
    return df


def train(df: pd.DataFrame, output_model_dir: str, params: Any):
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
    gbm = lgb.train(
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
    y_pred = gbm.predict(X_test, num_iteration=gbm.best_iteration)
    score = mean_squared_error(y_test, y_pred, squared=False)
    print(f"RMSE Score on Test set: {score:0.3f}")

    # Save model
    gbm.save_model(output_model_dir + "test_model.txt")
    print("Model saved")


@hydra.main(version_base=None, config_path="../train_configs", config_name="config")
def main(cfg: DictConfig):
    file_path = get_minio_data(cfg.get("data_dir"))
    df = get_df(file_path, cfg.get("date_format"))
    train_params = OmegaConf.to_object(cfg.get("train_params"))
    print(train_params, type(train_params))
    train(df, cfg.get("output_model_dir"), train_params)


if __name__ == "__main__":
    main()
