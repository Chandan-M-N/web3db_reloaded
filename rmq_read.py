import pika
import json

# Queue name
RABBITMQ_QUEUE = 'medical_data_queue'

# RabbitMQ connection parameters
RABBITMQ_HOST = '172.20.24.155'
RABBITMQ_USER = 'web3db'
RABBITMQ_PASSWORD = 'web3db'

def on_message_received(channel, method, properties, body):
    """
    Callback function that processes incoming messages.
    """
    print(f"Received message: {body}")
    # Acknowledge the message
    channel.basic_ack(delivery_tag=method.delivery_tag)

def consume_from_rabbitmq():
    """
    Connects to RabbitMQ and starts consuming messages from the queue.
    """
    try:
        # Establish a connection to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            )
        )
        print("Connected to RabbitMQ")

        # Create a channel
        channel = connection.channel()

        # Declare the quorum queue
        print(f"Declaring quorum queue: {RABBITMQ_QUEUE}")
        channel.queue_declare(
            queue=RABBITMQ_QUEUE,
            durable=True,
            arguments={"x-queue-type": "quorum"}
        )

        # Set up a consumer
        print(f"Starting consumer for queue: {RABBITMQ_QUEUE}")
        channel.basic_consume(
            queue=RABBITMQ_QUEUE,
            on_message_callback=on_message_received,
            auto_ack=False  # Manually acknowledge messages
        )

        # Start consuming messages
        print("Waiting for messages. To exit, press CTRL+C")
        channel.start_consuming()

    except Exception as e:
        print(f"Error in consume_from_rabbitmq: {e}")
    finally:
        if connection and not connection.is_closed:
            connection.close()
            print("Connection closed.")

if __name__ == '__main__':
    consume_from_rabbitmq()