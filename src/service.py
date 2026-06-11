from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.sql import SparkSession

from config import AppConfig
from datamart_client import DataMartClient


class FoodClusterService:
    def __init__(self, config: AppConfig):
        self.config = config

    def _create_spark(self) -> SparkSession:
        s = self.config.spark
        spark = (
            SparkSession.builder.appName(s.app_name)
            .master(s.master)
            .config("spark.driver.memory", s.driver_memory)
            .config("spark.sql.shuffle.partitions", s.shuffle_partitions)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel(s.log_level)
        return spark

    def fit(self):
        spark = self._create_spark()
        datamart = DataMartClient(self.config.datamart.url)
        try:
            df, feature_cols, total_rows, working_rows = datamart.load_training_data(
                spark=spark,
                input_path=self.config.data.input_path,
                min_non_null_ratio=self.config.preprocessing.min_non_null_ratio,
                target_rows=self.config.preprocessing.target_rows,
                seed=self.config.training.seed,
            )
            print(f"Loaded preprocessed data from data mart: rows={working_rows} / total={total_rows}")

            imputed_cols = [f"{c}_imp" for c in feature_cols]
            pipeline = Pipeline(
                stages=[
                    Imputer(
                        inputCols=feature_cols,
                        outputCols=imputed_cols,
                        strategy=self.config.preprocessing.imputer_strategy,
                    ),
                    VectorAssembler(inputCols=imputed_cols, outputCol="features_raw"),
                    StandardScaler(
                        inputCol="features_raw",
                        outputCol="features",
                        withMean=True,
                        withStd=True,
                    ),
                    KMeans(
                        featuresCol="features",
                        predictionCol="prediction",
                        k=self.config.training.k,
                        seed=self.config.training.seed,
                    ),
                ]
            )
            predictions = pipeline.fit(df).transform(df)

            score = ClusteringEvaluator(
                metricName=self.config.training.metric_name,
                distanceMeasure=self.config.training.distance_measure,
            ).evaluate(predictions)
            print(f"fit. {self.config.training.metric_name}={score:.4f}")

            predictions_df = predictions.select(*feature_cols, "prediction")
            metrics = {
                "metric": self.config.training.metric_name,
                "value": float(score),
            }
            datamart.save_training_results(predictions_df, metrics)
        finally:
            spark.stop()
