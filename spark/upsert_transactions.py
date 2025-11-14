from pyspark.sql import SparkSession
from datetime import datetime

spark = (
    SparkSession.builder.appName("HudiUpsert")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0")
    .getOrCreate()
)

import os
output_path = os.path.abspath("warehouse/hudi_transactions")

new_data = [
    (4, 101, 250.00, "USD", datetime.now().isoformat()),
    (5, 106, 1200.00, "USD", datetime.now().isoformat())
]

df_new = spark.createDataFrame(new_data, ["txn_id", "customer_id", "amount", "currency", "timestamp"])

df_new.write.format("hudi") \
    .option("hoodie.table.name", "hudi_transactions") \
    .option("hoodie.datasource.write.recordkey.field", "txn_id") \
    .option("hoodie.datasource.write.precombine.field", "timestamp") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .mode("append") \
    .save(output_path)

print("Upsert completed.")
spark.stop()
