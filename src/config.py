from dataclasses import dataclass
import yaml


@dataclass
class SparkConfig:
    app_name: str
    master: str
    driver_memory: str
    shuffle_partitions: str
    log_level: str


@dataclass
class DataConfig:
    input_path: str


@dataclass
class DataMartConfig:
    url: str


@dataclass
class PreprocessingConfig:
    min_non_null_ratio: float
    target_rows: int
    imputer_strategy: str


@dataclass
class TrainingConfig:
    k: int
    seed: int
    metric_name: str
    distance_measure: str


@dataclass
class AppConfig:
    spark: SparkConfig
    data: DataConfig
    datamart: DataMartConfig
    preprocessing: PreprocessingConfig
    training: TrainingConfig


def load_config(path: str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return AppConfig(
        spark=SparkConfig(**data["spark"]),
        data=DataConfig(**data["data"]),
        datamart=DataMartConfig(**data["datamart"]),
        preprocessing=PreprocessingConfig(**data["preprocessing"]),
        training=TrainingConfig(**data["training"]),
    )
