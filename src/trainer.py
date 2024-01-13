from sklearn.metrics import mean_squared_error
from lightgbm import LGBMRegressor, early_stopping, plot_importance
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import hydra
from omegaconf import DictConfig


def train(cfg: DictConfig):
    df = pd.read_csv('data/train_data.csv', sep=';', header=0)
    df = df.set_index('date')
    df.index = pd.to_datetime(df.index, format=cfg.get("date_format"))
    
    df['source']=df['source'].astype("category")
    df['destination']=df['destination'].astype("category")
    
    
    df['start_time'] = pd.to_datetime(df['start_time'], format='%H:%M')
    df['end_time'] = pd.to_datetime(df['end_time'], format='%H:%M')

    df['hour_start_time'] = df["start_time"].dt.hour
    df['hour_end_time'] = df["end_time"].dt.hour
    df=df.drop(['start_time', 'end_time'], axis=1)

    #Features from date index Time series
    df['dayofweek'] = df.index.dayofweek
    df['month'] = df.index.month
    df['year'] = df.index.year
    df['dayofyear'] = df.index.dayofyear
    df['dayofmonth'] = df.index.day
    df['weekofyear'] = df.index.isocalendar().week
    df['weekofyear']=df['weekofyear'].astype("int32")

    df['duration'] = pd.to_datetime(df['duration'], format='%H:%M:%S')
    df['duration'] = df['duration'].dt.hour * 60 + df['duration'].dt.minute + df['duration'].dt.second / 60
    df['duration'] = df['duration'].astype("int32")

    df['price'] = df['price'].astype("float32")
    df['stops'] = df['stops'].astype("int32")

    train = df.loc[df.index < cfg.get("split_date")]
    test = df.loc[df.index >= cfg.get("split_date")]

    # Split data into features and target
    X_train, y_train=train.drop('price', axis=1), train['price']
    X_test, y_test=test.drop('price', axis=1), test['price']

    reg = LGBMRegressor(n_estimators=1000,
                       max_depth=3,
                       learning_rate=0.01)
    
    reg.fit(X_train, y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            categorical_feature="auto", callbacks=[early_stopping(50)])
    
    plot_importance(reg, figsize=(12, 10))
    plt.savefig("feature_importance.png")

    pred = reg.predict(X_test)
    score = np.sqrt(mean_squared_error(y_test, pred))
    print(f'RMSE Score on Test set: {score:0.3f}')

    # Save model
    reg.booster_.save_model(cfg.get("output_model_dir")+"test_model.txt")



@hydra.main(version_base=None, config_path="../configs", config_name="config")
def main(cfg: DictConfig):
    train(cfg)

    #if already trained, load model
    #reg = lgb.Booster(model_file=cfg.get("output_model_dir")+"test_model.txt")

if __name__ == "__main__":
    main()