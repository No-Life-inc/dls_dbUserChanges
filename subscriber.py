import pika
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
import json
import os

# Load environment variables from .env file
env_vars = load_dotenv(override=True)

# RabbitMQ connection parameters
RABBITMQ_URL = f"amqp://{os.getenv('RABBITUSER')}:{os.getenv('RABBITPW')}@{os.getenv('RABBITURL')}/%2F"
RABBITMQ_QUEUE = 'UserQueue'

# MSSQL connection parameters
MSSQL_CONN_STR = f"DRIVER={{SQL Server}};SERVER={os.getenv('MSSQL_SERVER')};DATABASE={os.getenv('MSSQL_DATABASE')};UID={os.getenv('MSSQL_USER')};PWD={os.getenv('MSSQL_PASSWORD')}"

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
    created_at = datetime.fromisoformat(user['created_at'])

    insert_user = "INSERT INTO users(user_guid, created_at) OUTPUT INSERTED.user_id VALUES (?, ?)"
    cursor.execute(insert_user, user['guid'], created_at, )
    user_id = cursor.fetchone()[0]

    # Insert the message into MSSQL
    # Note: Adjust the table name and column names according to your MSSQL database schema
    insert_user_info = "INSERT INTO user_info(first_name, last_name, email, created_at, user_id) VALUES (?,?,?,?,?)"
    cursor.execute(insert_user_info, user['first_name'], user['last_name'], user['email'], created_at, user_id, )

    conn.commit()

    print("Story inserted into MSSQL")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Start consuming messages
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
print('Waiting for new stories...')
channel.start_consuming()
