import findspark
findspark.init()

from pyspark.sql import SparkSession
import time
import os

os.environ['PGPASSWORD'] = 'sunlab'

spark = SparkSession.builder \
    .appName("PostgreSQL Connection with PySpark") \
    .config("spark.jars", "/home/sunlab/Downloads/postgresql-42.7.4.jar") \
    .master("spark://172.20.103.30:7077") \
    .getOrCreate()

url = "jdbc:postgresql://172.20.103.30:5432/postgres"

properties = {
    "user": "postgres",
    "password": "sunlab",
    "driver": "org.postgresql.Driver"
}



def read_data_from_postgres(table_name):
    df = spark.read.jdbc(url, table_name, properties=properties)
    df.show()
    time.sleep(5)
    return df

def analyze_data(df):
    # Example analysis: Count the number of records
    record_count = df.count()
    print(f"Total number of records: {record_count}")


def spark_select_query(query):
    # Use Spark to read and process the data
    table_name = query.split()[3]  # Assume table name is the 4th word in the SELECT query
    table_name = table_name[:-1]
    df = read_data_from_postgres(table_name)
    analyze_data(df)
