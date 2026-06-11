import argparse

from config import load_config
from service import FoodClusterService


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    FoodClusterService(load_config(args.config)).fit()


if __name__ == "__main__":
    main()
