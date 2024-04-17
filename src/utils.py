import numpy as np
import pandas as pd


def rmse(prediction, ground_truth):
    # Calculate squared differences
    squared_diff = (prediction - ground_truth) ** 2
    # Calculate mean of squared differences
    mean_squared_diff = np.mean(squared_diff)
    # Take square root to get RMSE
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
