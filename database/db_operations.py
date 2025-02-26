import psycopg2
from psycopg2 import pool
import time

# Read hosts from hosts.txt
def read_hosts(file_path):
    with open(file_path, 'r') as file:
        hosts = [line.strip() for line in file if line.strip()]
    return hosts

# Create a connection pool
def create_connection_pool(host, dbname, user, password, min_conn=1, max_conn=5):
    try:
        return psycopg2.pool.SimpleConnectionPool(
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
    INSERT INTO ipfs_hashes (cid, topic) VALUES (%s, %s);
    """
    return execute_query(insert_query, (cid, topic))

# Execute a query using the connection pool with retry logic
def execute_query(query, params=None, retries=3, delay=1):
    global connection_pool
    for attempt in range(retries):
        try:
            # Get a connection from the pool
            connection = connection_pool.getconn()
            cursor = connection.cursor()

            # Execute the query
            cursor.execute(query, params or ())

            # Commit changes for INSERT/UPDATE/DELETE queries
            if not query.strip().lower().startswith("select"):
                connection.commit()

            # Release the connection back to the pool
            cursor.close()
            connection_pool.putconn(connection)
            # Return True if the query succeeds
            return True
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
        
