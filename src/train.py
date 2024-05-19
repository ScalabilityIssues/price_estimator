from datetime import datetime
from typing import Any
from dotenv import load_dotenv
import lightgbm as lgb
import pandas as pd
import hydra, os

from omegaconf import DictConfig, OmegaConf
from minio_client import MinioClient
from minio.error import S3Error
from utils_prediction import rmse, build_flight_df


def train(df: pd.DataFrame, params: Any):
    """
    Train a LightGBM model using the provided DataFrame and parameters.

    Args:
        df (pd.DataFrame): The input DataFrame containing the training data.
        params (Any): The parameters for the LightGBM model.

    Returns:
        model: The trained LightGBM model.
    """
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
        params,
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


# Train the model and upload it to MinIO, if a message from RabbitMQ arrives (scraping).
# If the upload fails, write a log file to allow the resuming of the process later.
@hydra.main(version_base=None, config_path="../configs/train", config_name="config")
def main(cfg: DictConfig):
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_BUCKET_NAME_TRAINING = os.getenv("MINIO_BUCKET_NAME_TRAINING")
    MINIO_BUCKET_NAME_MODEL = os.getenv("MINIO_BUCKET_NAME_MODEL")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    client = MinioClient(
        MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )

    output_model_dir = os.listdir(cfg.get("output_model_dir"))
    if (
        len(output_model_dir) != 0
        and len([i for i, f in enumerate(output_model_dir) if ".csv" in f]) != 0
        and not cfg.get("force_training")
    ):
        latest_file = max(
            [f for f in output_model_dir if not f.endswith(".gitkeep")],
            key=os.path.getctime,
        )
        if not client.exists_file(MINIO_BUCKET_NAME_TRAINING, latest_file):
            print("New files found, uploading to MinIO bucket...")
            client.upload_file(
                bucket_name=MINIO_BUCKET_NAME_TRAINING,
                source_dir=output_model_dir,
                file_name=latest_file,
                content_type="application/csv",
            )
            print(
                f"Files uploaded successfully to MinIO bucket {MINIO_BUCKET_NAME_TRAINING} from {output_model_dir}"
            )
    elif not client.is_empty(MINIO_BUCKET_NAME_MODEL) and not cfg.get("force_training"):
        print("No model data found, downloading from MinIO bucket...")
        client.download_file(
            dest_dir=output_model_dir,
            bucket_name=MINIO_BUCKET_NAME_MODEL,
            latest=True,
        )
        print(f"Files downloaded successfully to {output_model_dir}")
    else:
        print(
            "No data found in MinIO bucket or `force_training=True`, starting training..."
        )
        train_data_dir = cfg.get("data_dir")
        model_out_dir = cfg.get("model_out_dir")
        date_format = cfg.get("date_format")
        train_params = OmegaConf.to_object(cfg.get("train_params"))

        train_filename = None
        if len(os.listdir(train_data_dir)) == 0:
            print(
                f"No training data found in the directory {train_data_dir}, trying to download from MinIO..."
            )
            train_file_obj = client.download_file(
                dest_dir=train_data_dir,
                bucket_name=MINIO_BUCKET_NAME_TRAINING,
                latest=True,
            )
            if not train_file_obj:
                print("No training data file found")
                return
            else:
                print(f"Training data downloaded successfully to {train_data_dir}")
                train_filename = train_file_obj.object_name
        else:
            train_filename = max(
                [
                    train_data_dir + f
                    for f in os.listdir(train_data_dir)
                    if not f.endswith(".gitkeep")
                ],
                key=os.path.getctime,
            )

        df = pd.read_csv(train_data_dir + train_filename, sep=";", header=0)
        df = build_flight_df(df, date_format=date_format)

        # Drop redundant or useless columns
        df = df.drop(["currency"], axis=1)
        df["price"] = df["price"].astype("float32")

        model: lgb.Booster = train(df, train_params)
        model_name = "model" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".txt"
        model.save_model(model_out_dir + model_name)
        print(f"Model saved in {model_out_dir + model_name}")

        client.upload_file(
            bucket_name=MINIO_BUCKET_NAME_MODEL,
            source_dir=model_out_dir,
            file_name=model_name,
            latest=False,
            content_type="application/txt",
        )
        print(
            f"Model uploaded successfully to MinIO bucket {MINIO_BUCKET_NAME_MODEL} from {model_out_dir}"
        )


if __name__ == "__main__":
    try:
        load_dotenv()
        main()
    except S3Error as exc:
        print("Error occurred in MinIO", exc)