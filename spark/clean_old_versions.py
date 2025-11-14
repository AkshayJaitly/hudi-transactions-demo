from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("HudiCleaner")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0")
    .getOrCreate()
)

print("Cleaner placeholder. Requires Spark SQL extension environment.")
spark.stop()
