# Data Lakehouse Boilerplate
A minimalistic boilerplate to serve a modern stack data lakehouse.<br>
Uses the following stack: <br>
* S3(Minio)
* Iceberg(S3 Table Format)
* Trino(For querying data)
* Spark(For inserting data at scale)

## Starting up
In order to start up the project, you must have docker running on your machine.<br>
Run the following command to start up:
```bash
docker-compose up -d
```

## Connecting to Trino
You must first set up a trino "Iceberg Connector" to serve the iceberg tables for a JDBC connection.
to do so, docker-compose copies the **trino_iceberg.properties** file into your trino config.<br>
An example connector would look lik the following:
```ini
connector.name=iceberg
iceberg.catalog.type=jdbc

# JDBC catalog configuration for PostgreSQL
# Uses 5432 Postgres port as it is from within the docker closed network and not from our machine
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://postgres:5432/<JDBC_DB>
iceberg.jdbc-catalog.connection-user=<JDBC_USER>
iceberg.jdbc-catalog.connection-password=<JDBC_PASSWORD>
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.catalog-name=<CATALOG_NAME> #Name of your choice
iceberg.jdbc-catalog.default-warehouse-dir=s3a://<LAKEHOUSE_DIRECTORY>/ #Directory on S3 configured for iceberg

# S3 configuration
fs.hadoop.enabled=false
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=<MINIO_S3_REGION>
s3.aws-access-key=<MINIO_ACCESS_KEY>
s3.aws-secret-key=<MINIO_SECRET_KEY>
s3.path-style-access=true

# File format configuration
iceberg.file-format=PARQUET
iceberg.compression-codec=GZIP
```
To connect to trino with third party IDE(Such as DataGrip), you must setup a JDBC connection.<br>
Default credentials to trino's JDBC connection:
```
username=trino
password=<BLANK>
```

## Connecting to Spark
An example of how to connect to spark is found within the **etl** folder.<br>
Spark connects to the JDBC metastore and interacts with Iceberg tables natively(using the right JARs)<br>
An example connection would look like the following:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Minio-Iceberg-With-JDBC-Metastore") \
    .master("local[*]") \
    .config("spark.jars", "<LIST_OF_JARS_SEP_BY_COMMA>") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.<CATALOG_NAME>", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.<CATALOG_NAME>.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
    .config("spark.sql.catalog.<CATALOG_NAME>.warehouse", "s3a://<LAKEHOUSE_DIRECTORY>/") \
    .config("spark.sql.catalog.<CATALOG_NAME>.uri", "jdbc:postgresql://localhost:9123/<JDBC_DB>") \
    .config("spark.sql.catalog.<CATALOG_NAME>.jdbc.user", "<JDBC_USER>") \
    .config("spark.sql.catalog.<CATALOG_NAME>.jdbc.password", "<JDBC_PASSWORD>") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "<MINIO_ACCESS_KEY>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<MINIO_SECRET_KEY>") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

tables = spark.sql("SHOW SCHEMAS in iceberg")
tables.show()
```
To connect to a remote spark machine - change **.master** property to the address of the machine

## Running Spark Locally
To run spark locally, you must first download **pyspark** using pip command:
```
pip install pyspark
```
You also need to have an Hadoop instance on your machine. For windows, you can download the following Hadoop binaries and add it to your environment path for Spark to work as it should: [WinUtils](https://github.com/steveloughran/winutils/tree/master)<br>
Latest binaries(Hadoop 3.0.0) would work fine.