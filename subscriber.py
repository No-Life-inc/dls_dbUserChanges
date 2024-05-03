import pika
import pyodbc
from dotenv import load_dotenv
from dateutil import parser
import json
import os

# Load environment variables from .env file
env_vars = load_dotenv(override=True)

# RabbitMQ connection parameters
RABBITMQ_URL = f"amqp://{os.getenv('RABBITUSER')}:{os.getenv('RABBITPW')}@{os.getenv('RABBITURL')}/%2F"
RABBITMQ_QUEUE = 'UserQueue'

# MSSQL connection parameters
MSSQL_CONN_STR = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv('DB_SERVER')};DATABASE={os.getenv('DB_FRONTEND')};UID={os.getenv('DB_USER')};PWD={os.getenv('DB_PASSWORD')}"
# Connect to MSSQL
conn = pyodbc.connect(MSSQL_CONN_STR)
cursor = conn.cursor()

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

# Define the callback function
# Define the callback function
def callback(ch, method, properties, body):
    print("Received:", body)

    # Prepare the story for insertion
    data = json.loads(body)
    user = data['user']
    userInfo = data['userInfo']
    created_at = parser.parse(user['created_at'])

    insert_user = "INSERT INTO users(user_guid, created_at) OUTPUT INSERTED.user_id VALUES (?, ?)"
    cursor.execute(insert_user, user['guid'], created_at, )
    user_id = cursor.fetchone()[0]

    # Insert the message into MSSQL
    # Note: Adjust the table name and column names according to your MSSQL database schema
    insert_user_info = "INSERT INTO user_info(first_name, last_name, email, created_at, user_id) VALUES (?,?,?,?,?)"
    cursor.execute(insert_user_info, userInfo['FirstName'], userInfo['LastName'], userInfo['Email'], created_at, user_id)

    conn.commit()

    print("Story inserted into MSSQL")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Start consuming messages
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
print('Waiting for new stories...')
channel.start_consuming()
