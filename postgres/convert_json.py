import subprocess
import os
import json
from simple_ddl_parser import DDLParser
import ipfsApi
import psycopg2
from psycopg2 import sql

# def check_sql_syntax(sql_query):
#     # Write the SQL query to a temporary file
#     temp_file = "temp_query.sql"
#     with open(temp_file, "w") as file:
#         file.write(sql_query)

#     # Run pgsanity on the SQL file
#     try:
#         result = subprocess.run(
#             ["pgsanity", temp_file], 
#             capture_output=True, 
#             text=True
#         )
        
#         # Check if pgsanity returned an error message (syntax error)
#         if result.returncode != 0:
#             return result.stdout + result.stderr
#         return True  # No syntax errors
#     finally:
#         # Clean up the temporary file
#         if os.path.exists(temp_file):
#             os.remove(temp_file)

# def get_sql_command_type(sql_query):
#     # Strip whitespace and split by space to identify the first keyword
#     first_keyword = sql_query.strip().split()[0].upper()
#     # Return the command type
#     return first_keyword













def parse_create_to_json(sql_query):
    # Parse the CREATE command and convert to JSON format using DDLParser
    parse_results = DDLParser(sql_query).run()
    return parse_results



sql_query = """
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department_id INTEGER REFERENCES departments(id) ON DELETE CASCADE
);
"""
r = parse_create_to_json(sql_query)
print(type(r))
print(type(r[0]['table_name']))






# def add_to_ipfs(parse_results):
#     api = ipfsApi.Client('127.0.0.1', 5001)

#     # Convert Python dict to JSON string
#     json_str = json.dumps(parse_results)
#     # print(json_str)

#     # Add the JSON data to IPFS
#     response = api.add_json(json_str)

#     # Print the CID of the uploaded data
#     print(f"Data uploaded to IPFS with CID: {response}")
#     return response


# def add_cid_to_table(cid, table_name):
#     # Connect to the PostgreSQL database
#     try:
#         connection = psycopg2.connect(
#             dbname="postgres",  # Replace with your database name
#             user="postgres",      # Replace with your database username
#             password="sunlab",  # Replace with your database password
#             host="localhost",  # Replace with your database host if needed
#             port="5432"  # Default PostgreSQL port
#         )

#         cursor = connection.cursor()

#         # Prepare the SQL query to insert the data into cid_table
#         insert_query = sql.SQL("""
#             INSERT INTO cid_table (cid, table_name)
#             VALUES (%s, %s)
#         """)

#         # Execute the insert query with the given cid and table_name
#         cursor.execute(insert_query, (cid, table_name))

#         # Commit the transaction to the database
#         connection.commit()

#         print(f"CID {cid} and table_name {table_name} inserted into cid_table.")

#     except Exception as error:
#         print(f"Error inserting CID and table_name into cid_table: {error}")
    
#     finally:
#         # Close the cursor and connection
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()


# Example SQL query


# # Step 1: Check if the SQL syntax is correct
# syntax_result = check_sql_syntax(sql_query)
# if syntax_result is True:
#     # Step 2: Get the command type if syntax is correct
#     command_type = get_sql_command_type(sql_query)
#     print(f"The command type is: {command_type}")
    
#     # Step 3: Parse and convert to JSON if it's a CREATE command
#     if command_type == "CREATE":
#         json_result = parse_create_to_json(sql_query)
#         print(type(json_result))
#         print("Parsed JSON output:")
#         print(json.dumps(json_result, indent=4))
#         if isinstance(json_result, str):
#             json_result = json.loads(json_result)
    
#         # Extract and return the table name from the JSON result
#         table_name = json_result[0].get("table_name")
#         cid = add_to_ipfs(json_result)
#         add_cid_to_table(cid,table_name)

# else:
#     print(f"Syntax error: {syntax_result}")





