import subprocess
import os
import json
import psycopg2
from psycopg2 import sql
import ipfsApi
# from postgres.spark_sht import spark_select_query

os.environ['PGPASSWORD'] = 'sunlab'

def get_db_connection():
    return psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="sunlab",
            host="localhost",
            port="5432"
        )

def add_to_ipfs(file_path):
    # Initialize the IPFS client
    api = ipfsApi.Client('127.0.0.1', 5001)

    # Add the file to IPFS
    response = api.add(file_path)
    cid = response['Hash']
    print(f"Data uploaded to IPFS with CID: {cid}")

    # Pin the CID using IPFS Cluster (assuming 'ipfs-cluster-ctl' is installed)
    subprocess.run(['ipfs-cluster-ctl', 'pin', 'add', cid])
    print(f"CID {cid} has been pinned to the IPFS Cluster.")
    
    return cid


def add_cid_to_table(cid, table_name):
    try:
        connection = get_db_connection()

        cursor = connection.cursor()

        # Prepare the SQL query to insert the data into cid_table
        insert_query = sql.SQL("""
            INSERT INTO cid_table (cid, table_name) VALUES (%s, %s)
        """)

        # Execute the insert query with the given cid and table_name
        cursor.execute(insert_query, (cid, table_name))

        # Commit the transaction to the database
        connection.commit()

        print(f"CID {cid} and table_name {table_name} inserted into cid_table.")

    except Exception as error:
        print(f"Error inserting CID and table_name into cid_table: {error}")
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def update_cid_in_table(cid, table_name):
    try:
        # Connect to the PostgreSQL database
        connection = get_db_connection()

        cursor = connection.cursor()

        # Prepare the SQL query to update the CID in cid_table
        update_query = sql.SQL("""
            UPDATE cid_table
            SET cid = %s
            WHERE table_name = %s
        """)

        # Execute the update query with the new cid and the specified table_name
        cursor.execute(update_query, (cid, table_name))
        rowcount = cursor.rowcount

        # Commit the transaction to the database
        connection.commit()

        if rowcount > 0:
            print(f"CID for table_name {table_name} updated successfully to {cid}.")
        else:
            print(f"No entry found for table_name {table_name} to update.")

    except Exception as error:
        print(f"Error updating CID for table_name {table_name} {error}")
    
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def fetch_cid_from_table(table_name):
    """
    Fetches the CID for the latest entry of a given table name from the cid_table.

    Parameters:
        table_name (str): The name of the table to fetch the CID for.

    Returns:
        str or None: The CID if found, otherwise None.
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        print("Connected, running select query")

        # Prepare the SQL query to fetch the CID for the latest entry
        select_query = sql.SQL("""
            SELECT cid
            FROM cid_table
            WHERE table_name = %s
            ORDER BY date_time DESC
            LIMIT 1;
        """)

        # Execute the query to fetch the CID
        cursor.execute(select_query, (table_name,))
        result = cursor.fetchone()

        if result:
            print(f"CID found: {result[0]}")
            return result[0]
        else:
            print(f"No CID found for table: {table_name}")
            return None
    except Exception as error:
        print(f"Error fetching CID: {error}")
        return None
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()



def recreate_table_from_sql_dump(table_name, cid):
    try:
        # Define the file path where the SQL dump will be saved
        dump_file_path = f"{table_name}_dump.sql"

        # Use subprocess to run the IPFS CLI and fetch the file
        subprocess.run(['ipfs', 'get', cid, '-o', dump_file_path], check=True)
        print(f"SQL dump file downloaded from IPFS with CID: {cid}")

        # Use subprocess to execute the SQL dump and recreate the table
        subprocess.run(
            ['psql', '-U', 'postgres', '-d', 'postgres', '-f', dump_file_path],
            check=True
        )
        print(f"Table {table_name} recreated successfully from SQL dump.")
        if os.path.exists(dump_file_path):
            os.remove(dump_file_path)
        
    except Exception as error:
        print(f"Error recreating table {table_name} from SQL dump {error}")
        if os.path.exists(dump_file_path):
            os.remove(dump_file_path)


def create_query(conn,cur,query):
    # Step 1: Handle CREATE query
    try:
        table_name = query.split()[2]  # Assume table name is the 3rd word in the CREATE query
        print(table_name)
        op = fetch_cid_from_table(table_name)
        if op != None:
            return False, "Table already exists!"
        cur.execute(query)
        conn.commit()

        print("Table created successfully.")

        # Step 2: Generate the SQL dump for the table
        subprocess.run(["pg_dump", "-U", "postgres", "-d", "postgres", "--table=" + table_name, "-f", f"{table_name}_dump.sql"], check=True)
        print(f"SQL dump created for {table_name}: {table_name}_dump.sql")

        # Step 5: Drop the table after the SQL dump
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        conn.commit()
        print(f"Table {table_name} dropped after processing.")

        # Step 3: Add SQL dump to IPFS and store CID
        file_path = f"{table_name}_dump.sql"
        cid = add_to_ipfs(file_path)

        # Step 4: Add the CID to the cid_table
        add_cid_to_table(cid, table_name)
        if os.path.exists(file_path):
            os.remove(file_path)
        return True, "Table created successfully!"
    
    except Exception as e:
        print(f"Error Handling create, {e}")
        return False, e
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def select_query(conn,cur,query):
    try:
        # Step 2: Handle SELECT query
        table_name = query.split()[3]  # Assume table name is the 4th word in the SELECT query
        print(table_name[-1])
        if table_name[-1] == ';':
            print("in here")
            table_name = table_name[:-1]
        
        print(f"Select for table {table_name}")
        cid = fetch_cid_from_table(table_name)
        print(f"{cid} fetched for table")

        if cid:
            # Step 3: Recreate the table using the SQL dump
            recreate_table_from_sql_dump(table_name, cid)
            print("table recreated in postgres")
            # Execute the SELECT query
            cur.execute(query)
            result = cur.fetchall()
            # op,msg = spark_select_query(query)

            # Assuming the column names are available in `cur.description`
            columns = [desc[0] for desc in cur.description]

            # Convert the result to a list of dictionaries
            result_list = [dict(zip(columns, row)) for row in result]

            # Convert the list of dictionaries to JSON
            result_json = json.dumps(result_list)
            # Clean up the tables

            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            conn.commit()
            print(f"Table {table_name} cleaned up after SELECT query.")
            return True, result_json
        print("no cid")
        return False, "Table does not exists!"

    except Exception as e:
        print(f"Error handling select query {e}")
        return False, e

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def drop_table(conn, cur, query):
    # Step 1: Handle the DROP operation
    try:
        table_name = query.split()[2]  # Assume the table name is the 3rd word in the CREATE query
        if table_name[-1] == ';':
            table_name = query.split()[2][:-1]
        op = fetch_cid_from_table(table_name)
        if op == None:
            return False, "Table does not exists!"
        print(f"Attempting to drop entry for table: {table_name}")

        # Step 2: Correct the DELETE query
        delete_query = "DELETE FROM cid_table WHERE table_name = %s"
        cur.execute(delete_query, (table_name,))
        conn.commit()

        print(f"Entry for table {table_name} removed from cid_table.")
        return True, f"Table deleted successfully"

    except Exception as e:
        print(f"Error handling the drop operation: {e}")
        return False, e

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()



def non_select_query(conn,cur,query):

    try:
        # Step 2: Handle non-SELECT query (INSERT, UPDATE, DELETE, etc.)
        table_name = query.split()[2]
        if table_name[-1] == ';':
            table_name = query.split()[2][:-1]  # Assume table name is the 3rd word in the non-SELECT query
        cid = fetch_cid_from_table(table_name)
        print(f"cid fetched from table {cid}")

        if cid:
            # Step 3: Recreate the table using the SQL dump
            recreate_table_from_sql_dump(table_name, cid)
            # Execute the non-SELECT query
            cur.execute(query)
            
            print(f"Query result: {cur.rowcount}")
            rowcount = cur.rowcount
            conn.commit()

            print(f"Query executed successfully: {query}")

            # Step 4: Generate the new SQL dump
            subprocess.run(["pg_dump", "-U", "postgres", "-d", "postgres", "--table=" + table_name, "-f", f"{table_name}_new_dump.sql"], check=True)

            file_path = f"{table_name}_new_dump.sql"

            # Step 5: Add the new SQL dump to IPFS and store CID
            new_cid = add_to_ipfs(file_path)

            # Step 6: Update the CID in the cid_table and delete the old CID from IPFS
            update_cid_in_table(new_cid, table_name)
            # Clean up the tables
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            conn.commit()
            print(f"Table {table_name} cleaned up after query execution.")
            if os.path.exists(file_path):
                os.remove(file_path)
            
            return True, f"Query Successful, {rowcount} row/rows affected"

        return False, "Table does not exists"
    except Exception as e:
        print(f"Error handling query{e}")
        return False, e

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def handle_query(query):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="sunlab",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        # Analyze the query type
        if query.strip().lower().startswith("create"):
            print("here for create")
            output,message = create_query(conn,cur,query)

        elif query.strip().lower().startswith("select"):
            output,message = select_query(conn,cur,query)
            
        
        elif query.strip().lower().startswith("drop"):
            output, message = drop_table(conn,cur,query)

        else:
            output, message = non_select_query(conn,cur,query)
        
        return output, message

    except Exception as e:
        print(f"Error handling query {e}")
        return False, e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


#######################################################################################################################################
# Patient data

