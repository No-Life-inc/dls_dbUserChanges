import pika
import pyodbc
from dotenv import load_dotenv
import json

# Load environment variables from .env file
env_vars = load_dotenv(override=True)

# RabbitMQ connection parameters
RABBITMQ_URL = f"amqp://{env_vars['RABBITUSER']}:{env_vars['RABBITPW']}@{env_vars['RABBITURL']}/%2F"
RABBITMQ_QUEUE = 'new_stories'

# MSSQL connection parameters
MSSQL_CONN_STR = f"DRIVER={{SQL Server}};SERVER={env_vars['MSSQL_SERVER']};DATABASE={env_vars['MSSQL_DATABASE']};UID={env_vars['MSSQL_USER']};PWD={env_vars['MSSQL_PASSWORD']}"

# Connect to MSSQL
conn = pyodbc.connect(MSSQL_CONN_STR)
cursor = conn.cursor()

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)


# Define the callback function
def callback(ch, method, properties, body):
    print("Received:", body)

    # Prepare the story for insertion
    user = json.loads(body)

    # Insert the message into MSSQL
    # Note: Adjust the table name and column names according to your MSSQL database schema
    insert_query = "INSERT INTO user_info (first_name, last_name, email, created_at, user_id) VALUES (?,?,?,?,?)"
    cursor.execute(insert_query, user['first_name'], user['last_name'], user['email'], user['created_at'], user['user_id'])

    conn.commit()

    print("Story inserted into MSSQL")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Start consuming messages
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
print('Waiting for new stories...')
channel.start_consuming()