import lightgbm as lgb
import os
from omegaconf import DictConfig
import hydra


#FIXME - IMPLEMENT THIS FUNCTION WITH GRPC
@hydra.main(version_base=None, config_path="../config", config_name="config")
def main(cfg: DictConfig):
    #if already trained, load model
    path=cfg.get("output_model_dir")+cfg.get("model_filename")
    if os.path.exists(path):
        reg = lgb.Booster(path)
        reg.predict()
    else:
        print("Model not found")


if __name__ == "__main__":
    main()