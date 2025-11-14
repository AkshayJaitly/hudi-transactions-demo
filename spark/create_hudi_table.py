from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder.appName("HudiCreate")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0")
    .getOrCreate()
)

input_path = os.path.abspath("data/transactions_sample.csv")
output_path = os.path.abspath("warehouse/hudi_transactions")

df = spark.read.csv(input_path, header=True, inferSchema=True)

df.write.format("hudi") \
    .option("hoodie.table.name", "hudi_transactions") \
    .option("hoodie.datasource.write.recordkey.field", "txn_id") \
    .option("hoodie.datasource.write.precombine.field", "timestamp") \
    .option("hoodie.datasource.write.operation", "insert") \
    .mode("overwrite") \
    .save(output_path)

print("Hudi table created at", output_path)
spark.stop()
