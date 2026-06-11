from pyspark.sql import SparkSession

def test_wordcount():
    spark = (
        SparkSession.builder
        .appName("WordCount")
        .master("local[*]")
        .getOrCreate()
    )

    counts = (
        spark.sparkContext.textFile("README.md")
        .flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[0])
    )

    for word, count in counts.collect():
        print(f"{word}: {count}")

    spark.stop()
