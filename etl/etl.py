from pyspark.sql import SparkSession
import os

# Variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Locate JARs required for Iceberg and JDBC
jar_dir = os.path.join(BASE_DIR, "spark_jars")
jar_files = [os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")]
spark_jars = ",".join(jar_files)

# Load the Iceberg library and JDBC driver - Offline
spark = SparkSession.builder \
    .appName("Minio-Iceberg-With-JDBC-Metastore") \
    .master("local[*]") \
    .config("spark.jars", spark_jars) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
    .config("spark.sql.catalog.iceberg.uri", "jdbc:postgresql://localhost:9123/iceberg_db") \
    .config("spark.sql.catalog.iceberg.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.iceberg.jdbc.password", "iceberg123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "7RUhs1A49nPZx1HFmpnk") \
    .config("spark.hadoop.fs.s3a.secret.key", "inDm2FskNzNUscP7S2u5afgd3nno5GPgiVsCW8Ya") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

tables = spark.sql("SHOW SCHEMAS in iceberg")
tables.show()