# Data Lakehouse Boilerplate
A minimalistic boilerplate to serve a modern stack data lakehouse.<br>
Uses the following stack: <br>
* S3(Minio) + Iceberg(S3 Table Format)
* Trino(For querying data)
* Spark(For inserting data at scale)
* Airflow(Task orchestration)

Includes an implementation of an ETL service, extracting data, processing it and loading into the datalake using Spark. Orchestrated by Airflow.
Project can be found in **scripts** folder. 

## Starting up
In order to start up the project, you must have docker running on your machine.<br>
Run the following command to start up:
```bash
docker-compose up -d
```
Exposed ports:
- Port 9090: Trino
- Port 9000: Minio S3
- Port 9001: Minio Admin UI
- Port 9123: Iceberg postgres(for JDBC connection)
- Port 8080: Airflow Webserver
- Port 8081: Airflow driver Spark UI
- Port 7077: Spark Master
- Port 8082: Spark Master UI
- Port 8083: Spark Worker UI

## Setting up The DataLake
To start working with the datalake, you must first generate a pair of Access/Secret Keys in Minio.<br>
To do so, head over to **localhost:9001** and login using default credentials:
```
username=minio
password=minio123
```
Head over to "Access Keys" and generate yourself a pair of keys.<br>
Replace the default settings that are set in **assets/trino_iceberg.properties**(Will require restart to docker-compose).<br>
You may use the same pair for Spark connection/set up different pairs for Spark/Trino.

## Extras - Connecting to Trino
**Note** - All those settings are already a part of the docker-compose file.<br>
You must first set up a trino "Iceberg Connector" to serve the iceberg tables for a JDBC connection.
to do so, docker-compose copies the **trino_iceberg.properties** file into your trino config.<br>
An example connector would look like the following:
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

## Extras - Connecting to Spark
Spark connects to the JDBC metastore and interacts with Iceberg tables natively(using the right JARs)<br>
An example connection would look like the following:
```python
from pyspark.sql import SparkSession

    session = SparkSession.builder \
        .appName("DataLake") \
        .config("spark.jars", get_spark_jars()) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.session.timeZone", "Asia/Jerusalem") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.codegen.wholeStage", "false") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", JDBC_URL) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.jdbc.user", JDBC_USERNAME) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.jdbc.password", JDBC_PASSWORD) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", S3_LAKEHOUSE_DIRECTORY) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    tables = session.sql("SHOW SCHEMAS in iceberg")
    tables.show()
```
Make sure you provide the necessary JARs for spark to run as expected.<br>
All JARs must match versions for each component of the datalake:

- Postgres(JDBC connection)
- Spark Iceberg(Must match both spark master(or local) and iceberg version of 3rd party modules like Trino)

A list of JARs is provided in **assets/spark_jars.txt**.

## Extras - Running spark locally
To run spark locally, you must first download **pyspark** using pip command:
```
pip install pyspark
```
You also need to have an Hadoop instance on your machine. For windows, you can download the following Hadoop binaries and add it to your environment path for Spark to work as it should: [WinUtils](https://github.com/cdarlint/winutils)<br>
Latest binaries(Hadoop 3.6.6) would work fine.