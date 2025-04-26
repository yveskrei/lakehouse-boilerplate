from pyspark.sql import SparkSession
import os

# Variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class Spark:
    def __init__(self):
        # Create Spark session
        self.session = None
        self.session = self.get_session()

    def __del__(self):
        # Stop Spark session
        if self.session is not None:
            try:
                self.session.stop()
            except Exception as e:
                print(f"Error stopping Spark session: {e}")

    def get_session(self):
        # Locate JARs required for Iceberg and JDBC
        jar_dir = os.path.join(BASE_DIR, "spark_jars")
        jar_files = [os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")]
        spark_jars = ",".join(jar_files)

        # Create Spark session
        session = SparkSession.builder \
            .appName("Spark-Iceberg") \
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
            .config("spark.sql.session.timeZone", "Asia/Jerusalem") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.sql.codegen.wholeStage", "false") \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "8g") \
            .getOrCreate()

        return session
    
    def merge_records(self, records: list[dict], target_table: str, merge_columns: list, set_columns: list = None):
        """
        Load bulk data onto a table using merge statement.
        Handles dropping duplicates and creating the sql query itself.

        target_table: str - The name of the target table in Iceberg.
        records: list - The records to be merged into the target table.
        merge_columns: list - The columns to be used for merging(On which we check if the record already exists).
        set_columns: list - Optional, The columns to be updated in the target table. If not passed, records that matched will be ignored.
        """
        SOURCE_TABLE = "records_source"

        try:
            # Create DataFrame from records
            dataframe = self.session.createDataFrame(records)

            # Drop exact duplicate rows
            dataframe = dataframe.dropDuplicates()

            # Drop duplicates by unique columns
            dataframe = dataframe.dropDuplicates(merge_columns)

            # Create temporary view for the DataFrame
            dataframe.createOrReplaceTempView(SOURCE_TABLE)

            # Create merge statement for query
            merge_statement = []
            for merge_column in merge_columns:
                merge_statement.append(f"target.{merge_column} = source.{merge_column}")
            merge_statement = " AND ".join(merge_statement)
            
            # Create set statement for query
            set_statement = []
            if set_columns is not None:
                for set_column in set_columns:
                    set_statement.append(f"{set_column} = source.{set_column}")
                set_statement = ", ".join(set_statement)
            
            # Extract columns for query
            raw_columns = ','.join(dataframe.columns)
            source_columns = ','.join([f"source.{column}" for column in dataframe.columns])
            
            # Build the SQL query
            query = f"""
                MERGE INTO iceberg.{target_table} AS target
                USING {SOURCE_TABLE} AS source
                ON {merge_statement}
                {f'WHEN MATCHED THEN UPDATE SET {set_statement}' if set_statement else ''}
                WHEN NOT MATCHED THEN 
                    INSERT ({raw_columns})
                    VALUES ({source_columns})
            """

            # Execute the SQL query
            self.session.sql(query)

            # Drop the temporary view
            self.session.catalog.dropTempView(SOURCE_TABLE)
        except Exception as e:
            print(f"Error during merge: {e}")
            raise Exception(f"Cannot merge records into {target_table} table")