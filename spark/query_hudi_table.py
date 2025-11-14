from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("HudiQuery")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0")
    .getOrCreate()
)

import os
df = spark.read.format("hudi").load(os.path.abspath("warehouse/hudi_transactions") + "/*")
df.show(truncate=False)
spark.stop()
