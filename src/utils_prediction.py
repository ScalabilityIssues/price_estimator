import numpy as np
import pandas as pd


def rmse(prediction, ground_truth):
    squared_diff = (prediction - ground_truth) ** 2
    mean_squared_diff = np.mean(squared_diff)
    rmse_val = np.sqrt(mean_squared_diff)
    return rmse_val


def extract_time_features(original_df: pd.DataFrame) -> pd.DataFrame:
    new_df = pd.DataFrame(original_df)
    new_df["duration"] = (
        new_df["end_time"] - new_df["start_time"]
    ).dt.total_seconds() / 60
    new_df["duration"] = new_df["duration"].astype("int32")

    new_df["hour_start_time"] = new_df["start_time"].dt.hour
    new_df["hour_end_time"] = new_df["end_time"].dt.hour
    new_df["minutes_start_time"] = new_df["start_time"].dt.minute
    new_df["minutes_end_time"] = new_df["end_time"].dt.minute

    # Features from date index Time series
    new_df["dayofweek"] = new_df.index.dayofweek
    new_df["month"] = new_df.index.month
    new_df["year"] = new_df.index.year
    new_df["dayofyear"] = new_df.index.dayofyear
    new_df["dayofmonth"] = new_df.index.day
    new_df["weekofyear"] = new_df.index.isocalendar().week
    new_df["weekofyear"] = new_df["weekofyear"].astype("int32")
    return new_df


def build_flight_df(
    df: pd.DataFrame,
    date_format: str = "%Y-%m-%d",
    hour_format: str = "%H:%M%z",
    utc: bool = True,
) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["date"], format=date_format)
    df = df.set_index("date")
    df["source"] = df["source"].astype("category")
    df["destination"] = df["destination"].astype("category")

    df["start_time"] = pd.to_datetime(df["start_time"], format=hour_format, utc=utc)
    df["end_time"] = pd.to_datetime(df["end_time"], format=hour_format, utc=utc)

    df = extract_time_features(df)
    df = df.drop(["start_time", "end_time"], axis=1)
    return df
