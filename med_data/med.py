import subprocess
import os
import json
import psycopg2
from psycopg2 import sql
from ipfs_content import ipfs_operations
from postgres import pos_cl
import secrets;

os.environ['PGPASSWORD'] = 'sunlab'

def get_db_connection():
    return psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="sunlab",
            host="localhost",
            port="5432"
        )

def create_dump(table_name,file_name, db_name="postgres", user="postgres", host="localhost", port=5432, output_dir="."):
    """
    Creates a SQL dump for a specific table in a PostgreSQL database using pg_dump.

    Parameters:
        table_name (str): The name of the table to dump.
        db_name (str): The name of the database (default is 'postgres').
        user (str): The username for the database (default is 'postgres').
        host (str): The database host (default is 'localhost').
        port (int): The database port (default is 5432).
        output_dir (str): The directory to save the dump file (default is current directory).

    Returns:
        bool: True if the dump is created successfully, False otherwise.
    """
    dump_file = f"dumps/{file_name}_dump.sql"
    try:
        # Construct the pg_dump command
        command = [
            "pg_dump",
            "-U", user,
            "-h", host,
            "-p", str(port),
            "-d", db_name,
            "--table=" + table_name,
            "-f", dump_file
        ]

        # Run the command
        subprocess.run(command, check=True, env={"PGPASSWORD": "your_password"})  # Replace with your password
        print(f"SQL dump created for {table_name}: {dump_file}")
        return True, dump_file

    except subprocess.CalledProcessError as e:
        print(f"Error creating dump for {table_name}: {e}")
        return False, e

    except Exception as e:
        print(f"Unexpected error: {e}")
        return False, e
    
def drop_table(table_name):
    """
    Drops a table from a PostgreSQL database if it exists.

    Parameters:
        table_name (str): The name of the table to drop.
        db_config (dict): A dictionary containing the database connection parameters.

    Returns:
        bool: True if the table was successfully dropped or didn't exist, False if an error occurred.
    """
    try:
        # Establish connection to the database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Create the SQL query to drop the table if it exists
        query = sql.SQL("DROP TABLE IF EXISTS {table_name}").format(
            table_name=sql.Identifier(table_name)
        )

        # Execute the query
        cursor.execute(query)
        conn.commit()
        print(f"Table '{table_name}' dropped successfully (if it existed).")
        return True

    except psycopg2.Error as e:
        print(f"Error dropping table '{table_name}': {e}")
        return False

    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def query_creation(med_type, data, username):
    """
    Creates a table dynamically in PostgreSQL.

    Parameters:
        med_type (str): The medical type (e.g., 'BP', 'HeartRate').
        data (dict): The JSON data containing keys that will become column names and their respective values.
        username (str): The username used to construct the table name.
    """
    table_name = f"{med_type}_{username}"  # Dynamic table name
    print(type(data))
    columns = data.keys()  # Keys in data become column names

    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()

        # Construct SQL to create the table with dynamic columns
        create_table_query = sql.SQL("""
            CREATE TABLE {table_name} (
                med_type_id SERIAL PRIMARY KEY,
                {columns}
            );
        """).format(
            table_name=sql.Identifier(table_name),
            columns=sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL("TEXT")) for col in columns
            )
        )
        cursor.execute(create_table_query)
        conn.commit()  # Commit the creation of the table
        print(f"Table '{table_name}' created successfully.")
        return True
    except Exception as e:
        print(f"Error while creating table: {str(e)}")
        return False
    finally:
        # Close connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def insert_med(med_type, data, username):
    """
    Inserts data into the dynamically created table in PostgreSQL.

    Parameters:
        med_type (str): The medical type (e.g., 'BP', 'HeartRate').
        username (str): The username used to construct the table name.
        data (dict): The JSON data containing column names and their respective values.
    """
    table_name = f"{med_type}_{username}"  # Table name derived dynamically
    print(type(data))
    columns = data.keys()  # Columns based on keys in data

    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()

        # Construct SQL to insert data dynamically
        insert_query = sql.SQL("""
            INSERT INTO {table_name} ({columns})
            VALUES ({values});
        """).format(
            table_name=sql.Identifier(table_name),
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
            values=sql.SQL(", ").join(sql.Placeholder() for _ in columns)
        )
        cursor.execute(insert_query, tuple(data.values()))
        conn.commit()  # Commit the insertion of data
        print(f"Data inserted into table '{table_name}' successfully.")
        return True
    except Exception as e:
        print(f"Error while inserting data: {str(e)}")
        return False
    finally:
        # Close connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()



def check_table_exists(med_type, data, username):
    """
    Checks if a table for the given data type and username exists in cid_table.

    Parameters:
        med_type (str): The type of medical data (e.g., 'BP', 'HeartRate').
        data (dict): The data to be processed.
        username (str): The username to check for in the table.

    Returns:
        bool: True if all operations succeed, False otherwise.
    """
    table_name = f"{med_type}_{username}"  # Construct table name
    dump_file_path = None
    dump_name = None

    try:
        if len(data) == 0:
            return "Data not sent to insert"
            # Fetch CID and file from IPFS
        hash = pos_cl.fetch_cid_from_table(table_name)

        if hash != None:
            fetch_op = ipfs_operations.fetch_file_from_ipfs_cluster(hash)
            if not fetch_op:
                print(f"Failed to fetch file from IPFS for CID {hash}.")
                return False

            # Restore table from SQL dump
            dump_file_path = f"dumps/{hash}.sql"
            subprocess.run(
                ['psql', '-U', 'postgres', '-d', 'postgres', '-f', dump_file_path],
                check=True
            )
            print(f"Table {table_name} recreated successfully from SQL dump.")

            # Delete dump file after restoring
            if os.path.exists(dump_file_path):
                os.remove(dump_file_path)

            # Insert new data
            insert_med(med_type, data, "sunlab")

            # Generate new dump and add to IPFS
            dump_op, dump_name = create_dump(f"{med_type}_sunlab", hash)
            if not dump_op:
                print("Failed to create SQL dump.")
                return False

            ipfs_op, new_hash = ipfs_operations.add_file_with_metadata(dump_name)
            if ipfs_op:
                pos_cl.add_cid_to_table(new_hash, f"{med_type}_sunlab")
                print(f"Updated CID in table for {table_name}: {new_hash}")
            else:
                print("Failed to add new dump to IPFS.")
                return False

        else:
            # Create table and insert data
            query_creation(med_type, data, "sunlab")
            insert_med(med_type, data, "sunlab")

            # Generate initial dump and add to IPFS
            hash_dump = secrets.token_urlsafe(32)
            dump_op, dump_name = create_dump(f"{med_type}_sunlab", hash_dump)
            if not dump_op:
                print("Failed to create initial SQL dump.")
                return False

            ipfs_op, hash = ipfs_operations.add_file_with_metadata(dump_name)
            if ipfs_op:
                pos_cl.add_cid_to_table(hash, f"{med_type}_sunlab")
                print(f"Added new CID to table for {table_name}: {hash}")
            else:
                print("Failed to add initial dump to IPFS.")
                return False

        # Clean up: drop the temporary table
        drop_table(f"{med_type}_sunlab")

        return True

    except Exception as e:
        print(f"Error: {str(e)}")
        return False

    finally:
        # Ensure resources are cleaned up
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        if dump_name and os.path.exists(dump_name):
            os.remove(dump_name)
        if dump_file_path and os.path.exists(dump_file_path):
            os.remove(dump_file_path)


def get_table_data_as_json(table_name):
    """
    Fetches all data from a specified table and returns it in JSON format.

    Parameters:
        table_name (str): The name of the table from which data is to be fetched.
    
    Returns:
        dict: The data from the table in JSON format.
    """
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()

        # Construct SQL query to fetch all data from the table
        query = sql.SQL("SELECT * FROM {table_name}").format(
            table_name=sql.Identifier(table_name)
        )
        
        cursor.execute(query)
        rows = cursor.fetchall()

        # Get column names (headers) from the table
        column_names = [desc[0] for desc in cursor.description]

        # Convert rows to list of dictionaries
        data = [dict(zip(column_names, row)) for row in rows]

        # Convert the list of dictionaries into JSON
        json_data = json.dumps(data, default=str)  # default=str handles any non-serializable objects like timestamps
        
        return json_data  # Return data as a JSON string
    
    except Exception as e:
        print(f"Error while fetching data: {str(e)}")
        return None
    
    finally:
        # Close connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def fetch_data_api(med_type):
    try:
        table_name = f"{med_type}_sunlab"
        # Fetch CID and file from IPFS
        hash = pos_cl.fetch_cid_from_table(table_name)
        print(hash)
        if hash != None:
            fetch_op = ipfs_operations.fetch_file_from_ipfs_cluster(hash)
            if not fetch_op:
                print(f"Failed to fetch file from IPFS for CID {hash}.")
                return False

            # Restore table from SQL dump
            dump_file_path = f"dumps/{hash}.sql"
            subprocess.run(
                ['psql', '-U', 'postgres', '-d', 'postgres', '-f', dump_file_path],
                check=True
            )
            print(f"Table {table_name} recreated successfully from SQL dump.")

            # Delete dump file after restoring
            if os.path.exists(dump_file_path):
                os.remove(dump_file_path)

            json_data = get_table_data_as_json(table_name)
            # Clean up: drop the temporary table
            drop_table(table_name)
            return json_data
            
        else:
            return "Data does not exists!!"
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

    finally:
        # Ensure resources are cleaned up
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()