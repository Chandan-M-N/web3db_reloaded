import psycopg2
from psycopg2 import pool
import time
from datetime import datetime
import hashlib

# Read hosts from hosts.txt
def read_hosts(file_path):
    with open(file_path, 'r') as file:
        hosts = [line.strip() for line in file if line.strip()]
    return hosts

# Create a connection pool
def create_connection_pool(host, dbname, user, password, min_conn=1, max_conn=10):
    try:
        return psycopg2.pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=26257,  # Default CockroachDB port
            sslmode='require'  # Use SSL for secure connections
        )
    except Exception as e:
        print(f"Failed to create connection pool for host {host}: {e}")
        return None

# Connect to the first available host
def connect_to_cluster(hosts, dbname, user, password):
    for host in hosts:
        print(f"Trying to connect to host: {host}")
        connection_pool = create_connection_pool(host, dbname, user, password)
        if connection_pool:
            print(f"Connected to host: {host}")
            return connection_pool
    raise Exception("Could not connect to any host in the list.")

# Initialize the connection pool when the module is loaded
hosts = read_hosts("hosts.txt")
dbname = "defaultdb"
user = "root"
password = "web3db"
connection_pool = None

def initialize_connection_pool():
    global connection_pool
    connection_pool = connect_to_cluster(hosts, dbname, user, password)

# Initialize the connection pool for the first time
initialize_connection_pool()

# Function to add a hash and topic to the ipfs_hashes table
def add_hash(cid, topic):
    insert_query = """
    INSERT INTO ipfs_hash (ipfs_hash, device_id) VALUES (%s, %s);
    """
    return execute_query(insert_query, (cid, topic))

# Execute a query using the connection pool with retry logic
def execute_query(query, params=None, retries=3, delay=1):
    global connection_pool  # Declare 'connection_pool' as global at the start of the function
    for attempt in range(retries):
        connection = None
        try:
            # Get a connection from the pool
            connection = connection_pool.getconn()
            cursor = connection.cursor()

            # Execute the query
            cursor.execute(query, params or ())

            # Commit changes for INSERT/UPDATE/DELETE queries
            if not query.strip().lower().startswith("select"):
                connection.commit()

            # Return True if the query succeeds
            return True
        except psycopg2.pool.PoolError as e:
            print(f"Connection pool exhausted: {e}")
            # Dynamically increase the pool size
            connection_pool = create_connection_pool(hosts[0], dbname, user, password, min_conn=1, max_conn=20)  # Increase max_conn
            print("Connection pool size increased.")
            continue
        except psycopg2.OperationalError as e:
            print(f"Connection error (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                # Reinitialize the connection pool
                initialize_connection_pool()
            else:
                # Return False if all retries fail
                return False
        except Exception as e:
            print(f"Error executing query: {e}")
            return False
        finally:
            # Release the connection back to the pool
            if connection:
                cursor.close()
                connection_pool.putconn(connection)


def fetch_cids_by_time(date_str, time_str, topic):
    """
    Fetch all CIDs from the ipfs_hashes table for a specific date and within the last specified time period.

    Args:
        date_str (str): A string representing the date in the format "YYYY-MM-DD" or "all".
        time_str (str): A string representing the time period (e.g., "15 mins", "2 days") or "all".
        topic (str): The topic to filter the CIDs by.

    Returns:
        list: A list of CIDs (strings), or an empty list if no rows are found.
        str: A status message indicating success or failure.
    """
    # Case 1: date = all, time = all
    if date_str.lower() == "all" and time_str.lower() == "all":
        query = """
        SELECT ipfs_hash 
        FROM ipfs_hash
        WHERE device_id = %s 
        ORDER BY date DESC, time DESC;
        """
        try:
            connection = connection_pool.getconn()
            cursor = connection.cursor()
            cursor.execute(query, (topic,))
            results = [row[0] for row in cursor.fetchall()]
            cursor.close()
            connection_pool.putconn(connection)
            if results:
                return results, 'Success'
            else:
                return results, 'No data available'
        except Exception as e:
            print(f"Error fetching CIDs: {e}")
            return [], str(e)

    # Case 2: date = some date, time = all
    if time_str.lower() == "all":
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            print("Invalid date format. Expected format: 'YYYY-MM-DD'")
            return [], "Invalid date format. Expected format: 'YYYY-MM-DD'"

        query = """
        SELECT ipfs_hash 
        FROM ipfs_hash
        WHERE device_id = %s 
          AND date = %s 
        ORDER BY date DESC, time DESC;
        """
        try:
            connection = connection_pool.getconn()
            cursor = connection.cursor()
            cursor.execute(query, (topic, date_str))
            results = [row[0] for row in cursor.fetchall()]
            cursor.close()
            connection_pool.putconn(connection)
            if results:
                return results, 'Success'
            else:
                return results, 'No data available'
        except Exception as e:
            print(f"Error fetching CIDs: {e}")
            return [], str(e)

    # Original case: date = some date, time = specific interval
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Expected format: 'YYYY-MM-DD'")
        return [], "Invalid date format. Expected format: 'YYYY-MM-DD'"

    time_parts = time_str.split()
    if len(time_parts) != 2:
        print("Invalid time format. Expected format: 'X sec(s)/min(s)/hour(s)/day(s)'")
        return [], "Invalid time format. Expected format: 'X sec(s)/min(s)/hour(s)/day(s)'"

    value, unit = time_parts
    try:
        value = int(value)
    except ValueError:
        print("Invalid time value. Expected an integer.")
        return [], "Invalid time value. Expected an integer."

    unit = unit.lower()
    if unit in ["sec", "secs", "second", "seconds"]:
        interval = f"{value} seconds"
    elif unit in ["min", "mins", "minute", "minutes"]:
        interval = f"{value} minutes"
    elif unit in ["hour", "hours"]:
        interval = f"{value} hours"
    elif unit in ["day", "days"]:
        interval = f"{value} days"
    else:
        print("Invalid time unit. Expected 'sec(s)', 'min(s)', 'hour(s)', or 'day(s)'.")
        return [], "Invalid time unit. Expected 'sec(s)', 'min(s)', 'hour(s)', or 'day(s)'."

    query = """
    SELECT ipfs_hash 
    FROM ipfs_hash
    WHERE device_id = %s 
      AND date = %s 
      AND time >= (NOW()::time - INTERVAL %s)
    ORDER BY date DESC, time DESC;
    """

    try:
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(query, (topic, date_str, interval))
        results = [row[0] for row in cursor.fetchall()]
        cursor.close()
        connection_pool.putconn(connection)
        if results:
            return results, 'Success'
        else:
            return results, 'No data available'
    except Exception as e:
        print(f"Error fetching CIDs: {e}")
        return [], str(e)


def hash_wallet_id(wallet_id):
    """
    Hash the wallet_id using SHA-256.
    """
    return hashlib.sha256(wallet_id.encode()).hexdigest()

def add_device(wallet_id, device_ids,names, category_list, measurement_unit_list):
    """
    Add a wallet_id (hashed) and associated device_ids to the device_list table.

    Args:
        wallet_id (str): The wallet ID to be hashed and stored.
        device_ids (list): A list of device IDs to be associated with the wallet ID.

    Returns:
        dict: A dictionary containing the status, message, and list of inserted device IDs.
    """
    try:
        # Hash the wallet_id
        hashed_wallet_id = hash_wallet_id(wallet_id)

        # Prepare the insert query
        insert_query = """
        INSERT INTO device_list (hash_of_wallet_id, device_id, name, category, measurement_unit)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (device_id) DO NOTHING; 
        """

        inserted_device_ids = []
        failed_device_ids = []

        # Loop through device_ids and insert them
        for x in range(len(device_ids)):
            # Execute the query
            success = execute_query(insert_query, (hashed_wallet_id, device_ids[x], names[x], category_list[x],measurement_unit_list[x]))
            if success:
                inserted_device_ids.append((device_ids[x], names[x], category_list[x],measurement_unit_list[x]))
            else:
                failed_device_ids.append(device_ids[x],names[x], category_list[x],measurement_unit_list[x])

        # Prepare the response
        if inserted_device_ids:
            return {
                "status": "success",
                "message": f"Successfully inserted {len(inserted_device_ids)} device IDs.",
                "inserted_device_ids": inserted_device_ids,
                "failed_device_ids": failed_device_ids,
            }
        else:
            return {
                "status": "error",
                "message": "No device IDs were inserted.",
                "failed_device_ids": failed_device_ids,
            }
    except Exception as e:
        return {
            "status": "error",
            "message": e
        }
    

    
# Add the get_user_devices function to your existing code
def get_user_devices(wallet_id):
    """
    Fetch all device IDs associated with a given wallet ID (hashed) from the device_list table.

    Args:
        wallet_id (str): The wallet ID (hashed) to fetch devices for.

    Returns:
        dict: A dictionary containing the status, message, and list of device IDs.
    """
    try:
        # Prepare the select query
        select_query = """
        SELECT device_id,name,category,measurement_unit 
        FROM device_list
        WHERE hash_of_wallet_id = %s;
        """

        # Execute the query
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(select_query, (wallet_id,))
        # Fetch all rows and convert them into a list of dictionaries
        columns = [desc[0] for desc in cursor.description]  # Get column names
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        connection_pool.putconn(connection)

        # Prepare the response
        if results:
            return results
        else:
            return []
        
    except Exception as e:
        print(e)
        return []
