import json
import pika
from pika.adapters.blocking_connection import BlockingConnection
from pika.connection import ConnectionParameters
from pika.credentials import PlainCredentials
from queue import Queue

# File paths
HOSTS_FILE = "hosts.txt"
USER_FILE = "rabbitmq_config/user.json"

# Read RabbitMQ hosts from hosts.txt
def load_hosts():
    try:
        with open(HOSTS_FILE, "r") as file:
            return [line.strip() for line in file if line.strip()]
    except Exception as e:
        print(f"Error reading {HOSTS_FILE}: {e}")
        return []

# Read RabbitMQ user credentials from user.json
def load_user_credentials():
    try:
        with open(USER_FILE, "r") as file:
            credentials = json.load(file)
            return credentials.get("username"), credentials.get("password")
    except Exception as e:
        print(f"Error reading {USER_FILE}: {e}")
        return None, None

# Get hosts and credentials
rabbitmq_nodes = load_hosts()
username, password = load_user_credentials()

# Queue name
RABBITMQ_QUEUE = 'medical_data_queue'

# Connection pool
_connection_pool = None

def get_connection_pool():
    """
    Returns a RabbitMQ connection pool.
    """
    global _connection_pool
    if _connection_pool is None:
        # Initialize the connection pool
        _connection_pool = Queue(maxsize=2)  # Adjust maxsize as needed
        for _ in range(2):  # Pre-fill the pool with connections
            connection = create_connection()
            if connection:
                _connection_pool.put(connection)
    return _connection_pool

def create_connection():
    """
    Creates a new RabbitMQ connection by trying each node in the cluster sequentially.

    Args:
        rabbitmq_nodes (list): List of RabbitMQ node hosts.
        username (str): RabbitMQ username.
        password (str): RabbitMQ password.

    Returns:
        pika.BlockingConnection: A RabbitMQ connection if successful, otherwise None.
    """
    for host in rabbitmq_nodes:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host,  # Use the current host
                    credentials=pika.PlainCredentials(username, password),
                    heartbeat=600,  # Increase heartbeat to avoid timeouts
                    blocked_connection_timeout=300,  # Timeout for blocked connections
                )
            )
            print(f"Successfully connected to RabbitMQ node: {host}")
            return connection
        except Exception as e:
            print(f"Failed to connect to RabbitMQ node {host}: {e}")
            continue  # Try the next node

    # If no connection was successful
    print("Failed to connect to all RabbitMQ nodes.")
    return None

def get_connection():
    """
    Returns a RabbitMQ connection from the pool.
    """
    connection_pool = get_connection_pool()
    connection = connection_pool.get()

    # Check if the connection is valid
    if connection and connection.is_closed:
        print("Connection is closed. Creating a new one.")
        connection = create_connection()

    return connection

def release_connection(connection):
    """
    Releases a RabbitMQ connection back to the pool.
    """
    if connection and not connection.is_closed:
        connection_pool = get_connection_pool()
        connection_pool.put(connection)
    else:
        print("Connection is closed. Discarding it.")

def send_to_rabbitmq(message):
    """
    Sends a message to the RabbitMQ quorum queue using a connection pool.
    """
    if not rabbitmq_nodes or not username or not password:
        print("Missing RabbitMQ configuration. Check hosts.txt and user.json.")
        return

    connection = None
    try:
        # Get a connection from the pool
        connection = get_connection()
        if not connection:
            print("Failed to get a valid connection.")
            return

        channel = connection.channel()

        # Declare the quorum queue
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True, arguments={"x-queue-type": "quorum"})

        # Publish the message
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            )
        )

        print(f" [x] Sent to RabbitMQ: {message}")

    except Exception as e:
        print(f"Failed to send message to RabbitMQ: {e}")
    finally:
        # Release the connection back to the pool
        if connection:
            release_connection(connection)