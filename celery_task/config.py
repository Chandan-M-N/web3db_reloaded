from kombu import Queue

# Define the task queues with quorum settings
task_queues = [
    Queue("medical_data_queue", queue_arguments={"x-queue-type": "quorum"})  # Set the queue type to 'quorum'
]

# Define the task routes to ensure tasks are routed to the quorum queue
task_routes = {
    'tasks.consume_task': 'medical_data_queue',  # Route the 'consume_task' to 'medical_data_queue'
}
