import findspark
findspark.init()

from pyspark.sql import SparkSession
import os
import json

os.environ['PGPASSWORD'] = 'sunlab'

spark = SparkSession.builder \
    .appName("PostgreSQL Connection with PySpark") \
    .config("spark.jars", "/home/sunlab/Downloads/postgresql-42.7.4.jar") \
    .master("spark://172.20.24.155:7077") \
    .getOrCreate()


url = "jdbc:postgresql://172.20.24.155:5432/postgres"

properties = {
    "user": "postgres",
    "password": "sunlab",
    "driver": "org.postgresql.Driver"
}



def read_data_from_postgres(table_name):
    df = spark.read.jdbc(url, table_name, properties=properties)
    print("data fetched from postgres")
    df.show()
    return df

def analyze_data(df, query):
    # Register the DataFrame as a temporary table
    df.createOrReplaceTempView("temp_table")

    # Run the query using Spark SQL
    result_df = spark.sql(query)

    # Show the result of the query
    result_df.show()
     # Convert each row of the DataFrame into a JSON string
    json_strings = result_df.toJSON().collect()

    # Convert the JSON strings into a Python list of dictionaries
    json_list = [json.loads(json_str) for json_str in json_strings]

    # Example analysis: Count the number of records in the result
    record_count = result_df.count()
    print(f"Total number of records in query result: {record_count}")
    return True, json_list


def spark_select_query(query):
    try:
        # Use Spark to read and process the data
        table_name = query.split()[3]  # Assume table name is the 4th word in the SELECT query
        if table_name[-1] == ';':
            table_name = table_name[:-1]
        print(f"running spark select for table {table_name}")
        df = read_data_from_postgres(table_name)
        op,msg = analyze_data(df,query)
        return op, msg
    except Exception as e:
        return False, e
