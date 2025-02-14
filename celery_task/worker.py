from celery import Celery, bootsteps
from kombu import Queue

# Define the custom QoS class to disable global QoS for quorum queues
class NoChannelGlobalQoS(bootsteps.StartStopStep):
    requires = {'celery.worker.consumer.tasks:Tasks'}

    def start(self, c):
        # Disable global QoS by setting it to False
        qos_global = False

        # Apply the basic QoS settings to the default channel
        c.connection.default_channel.basic_qos(0, c.initial_prefetch_count, qos_global)

        # Set the prefetch count method manually
        def set_prefetch_count(prefetch_count):
            return c.task_consumer.qos(
                prefetch_count=prefetch_count,
                apply_global=qos_global,
            )

        # Assign the custom QoS object
        c = set_prefetch_count(c.initial_prefetch_count)

# Create Celery app instance
app = Celery("worker",
    broker="pyamqp://web3db:web3db@localhost//",
    task_queues=[
        Queue("medical_data_queue", queue_arguments={"x-queue-type": "quorum"})
    ],
    worker_prefetch_multiplier=1,  # Ensure prefetch is set to 1 for quorum queues
    task_acks_late=True,           # Acknowledge tasks after they are processed
    task_reject_on_worker_lost=True,  # Reject tasks if the worker is lost
    )

# Add the custom QoS step to the Celery worker's 'consumer' steps
app.steps['consumer'].add(NoChannelGlobalQoS)

# Load the configuration from your config file (if you have one)
app.config_from_object('config')


# Define a simple Celery task
@app.task(name="tasks.consume_task")
def consume_task(message):
    print(f"Consumed Message: {message}")

if __name__ == "__main__":
    print("Starting Celery worker...")
    app.worker_main(["worker", "--loglevel=info", "-Q", "medical_data_queue"])
