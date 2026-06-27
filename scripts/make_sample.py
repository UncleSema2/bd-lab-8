import sys
from pathlib import Path
from pyspark.sql import SparkSession
import shutil


def main():
    src = sys.argv[1]
    dst = sys.argv[2]
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 20000

    spark = (
        SparkSession.builder.appName("make-sample")
        .master("local[8]")
        .config("spark.driver.memory", "16g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = spark.read.parquet(src)
    cols = raw.columns
    print(f"source columns={len(cols)}")

    sample = raw.select(*cols).limit(n)
    out = Path(dst)
    if out.exists():
        shutil.rmtree(out)
    sample.repartition(1).write.mode("overwrite").parquet(str(out))

    print(f"wrote {n} rows to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
