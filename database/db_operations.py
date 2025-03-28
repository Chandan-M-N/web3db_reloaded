import psycopg2
from psycopg2 import pool
import time
from datetime import datetime

# Read hosts from hosts.txt
def read_hosts(file_path):
    with open(file_path, 'r') as file:
        hosts = [line.strip() for line in file if line.strip()]
    return hosts

# Create a connection pool
def create_connection_pool(host, dbname, user, password, min_conn=1, max_conn=5):  # Reduced max_conn
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
    global connection_pool
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
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise e
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

# Fetch CIDs by time
def fetch_cids_by_time(date_str, time_str, topic):
    """
    Fetch all CIDs from the ipfs_hashes table for a specific date and within the last specified time period.
    """
    connection = None
    try:
        # Case 1: date = all, time = all
        if date_str.lower() == "all" and time_str.lower() == "all":
            query = """
            SELECT ipfs_hash 
            FROM ipfs_hash
            WHERE device_id = %s 
            ORDER BY date DESC, time DESC;
            """
            connection = connection_pool.getconn()
            cursor = connection.cursor()
            cursor.execute(query, (topic,))
            results = [row[0] for row in cursor.fetchall()]
            if results:
                return results, 'Success'
            else:
                return results, 'No data available'

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
            connection = connection_pool.getconn()
            cursor = connection.cursor()
            cursor.execute(query, (topic, date_str))
            results = [row[0] for row in cursor.fetchall()]
            if results:
                return results, 'Success'
            else:
                return results, 'No data available'

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

        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(query, (topic, date_str, interval))
        results = [row[0] for row in cursor.fetchall()]
        if results:
            return results, 'Success'
        else:
            return results, 'No data available'
    except Exception as e:
        print(f"Error fetching CIDs: {e}")
        return [], str(e)
    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)


# Add device
def add_device(wallet_id, device_ids, names, category_list, measurement_unit_list):
    """
    Add a wallet_id (hashed) and associated device_ids to the device_list table.
    """
    try:

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
            success = execute_query(insert_query, (wallet_id, device_ids[x], names[x], category_list[x], measurement_unit_list[x]))
            if success:
                inserted_device_ids.append((device_ids[x], names[x], category_list[x], measurement_unit_list[x]))
            else:
                failed_device_ids.append((device_ids[x], names[x], category_list[x], measurement_unit_list[x]))

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
            "message": str(e)
        }

# Get user devices
def get_user_devices(wallet_id):
    """
    Fetch all device IDs associated with a given wallet ID (hashed) from the device_list table.
    """
    connection = None
    try:
        # Prepare the select query
        select_query = """
        SELECT device_id, name, category, measurement_unit 
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
        if results:
            return results
        else:
            return []
    except Exception as e:
        print(e)
        return []
    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)


def check_device_exists(wallet_id, device_id):
    """
    Check if a specific wallet_id and device_id combination exists in the device_list table.
    Returns True if the row exists, otherwise False.
    """
    connection = None
    try:
        # Prepare the select query
        select_query = """
        SELECT 1 
        FROM device_list
        WHERE hash_of_wallet_id = %s AND device_id = %s;
        """

        # Execute the query
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(select_query, (wallet_id, device_id))
        
        # Fetch the result
        result = cursor.fetchone()
        
        # If a row is found, return True; otherwise, return False
        return result is not None
    except Exception as e:
        print(f"Error checking device existence: {e}")
        return False
    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)


def get_user_profile_by_wallet_id(wallet_id):
    """
    Fetch all user profile data for a given wallet_id.
    """
    connection = None
    try:
        # Prepare the select query
        select_query = """
        SELECT wallet_id, name, email, height, weight, age, gender, bmi
        FROM user_profile
        WHERE wallet_id = %s;
        """

        # Execute the query
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(select_query, (wallet_id,))

        # Fetch the result
        result = cursor.fetchone()

        if result:
            # Convert the result to a dictionary
            columns = [desc[0] for desc in cursor.description]  # Get column names
            user_profile = dict(zip(columns, result))
            return user_profile
        else:
            return None
    except Exception as e:
        print(f"Error fetching user profile: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)



def get_wallet_id_by_email(email):
    """
    Retrieve the wallet_id for a given email.
    """
    connection = None
    try:
        # Prepare the select query
        select_query = """
        SELECT wallet_id
        FROM user_profile
        WHERE email = %s;
        """

        # Execute the query
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(select_query, (email,))

        # Fetch the result
        result = cursor.fetchone()

        if result:
            return result[0]  # Return the wallet_id
        else:
            return None
    except Exception as e:
        print(f"Error retrieving wallet_id: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)


def add_user_profile(wallet_id, name, email, height, weight, age, gender, bmi):
    """
    Add or update user profile data in the user_profile table.
    """
    connection = None
    try:
        # Prepare the insert/update query
        insert_query = """
        INSERT INTO user_profile (wallet_id, name, email, height, weight, age, gender, bmi)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (wallet_id) DO UPDATE
        SET name = EXCLUDED.name,
            email = EXCLUDED.email,
            height = EXCLUDED.height,
            weight = EXCLUDED.weight,
            age = EXCLUDED.age,
            gender = EXCLUDED.gender,
            bmi = EXCLUDED.bmi;
        """

        # Execute the query
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(insert_query, (wallet_id, name, email, float(height), float(weight), int(age), gender, float(bmi)))
        connection.commit()

        # Return True if the query succeeds
        return True
    except Exception as e:
        print(f"Error adding/updating user profile: {e}")
        return False
    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)


def add_encryption_key(wallet_id: str, encrypted_key: bytes) -> bool:
    """
    Add an encrypted key in the data_owner_key table.
    
    Args:
        wallet_id: The wallet ID (string)
        encrypted_key: The encryption key (encrypted with owner's public key) as bytes
    
    Returns:
        bool: True if successful, False if failed
    """
    connection = None
    try:
        # Prepare the insert/update query
        query = """
        INSERT INTO data_owner_key (wallet_id, encrypted_key)
        VALUES (%s, %s)
        ON CONFLICT (wallet_id) DO UPDATE
        SET encrypted_key = EXCLUDED.encrypted_key;
        """

        # Execute the query
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(query, (wallet_id, encrypted_key))
        connection.commit()
        
        return True
    except Exception as e:
        print(f"Error adding/updating encryption key: {e}")
        return False
    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)


def get_encryption_key(wallet_id: str) -> bytes:
    """
    Retrieve the encrypted key for a given wallet ID.
    
    Args:
        wallet_id: The wallet ID to look up
    
    Returns:
        bytes: The encrypted key if found, None if not found or error
    """
    connection = None
    try:
        # Prepare the select query
        query = """
        SELECT encrypted_key FROM data_owner_key
        WHERE wallet_id = %s;
        """

        # Execute the query
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(query, (wallet_id,))
        result = cursor.fetchone()
        
        return result[0] if result else None
    except Exception as e:
        print(f"Error retrieving encryption key: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)


def get_wallet_id_by_device(device_id: str) -> str:
    """
    Fetch the wallet_id associated with a given device_id from the device_list table.
    
    Args:
        device_id: The device ID to look up
    
    Returns:
        str: The wallet_id if found, None if not found or error
    """
    connection = None
    try:
        # Prepare the select query
        select_query = """
        SELECT hash_of_wallet_id
        FROM device_list
        WHERE device_id = %s;
        """

        # Execute the query
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(select_query, (device_id,))
        
        # Fetch the result
        result = cursor.fetchone()
        
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching wallet_id for device {device_id}: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)