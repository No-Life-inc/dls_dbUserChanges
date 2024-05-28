import pika
import pyodbc
from dotenv import load_dotenv
from dateutil import parser
from pymongo import MongoClient
import json
import os
import uuid
import logging

# Load environment variables from .env file
env_vars = load_dotenv(override=True)
logging.basicConfig(level=logging.INFO)

# RabbitMQ connection parameters
RABBITMQ_URL = f"amqp://{os.getenv('RABBITUSER')}:{os.getenv('RABBITPW')}@{os.getenv('RABBITURL')}/%2F"
RABBITMQ_NEW_USER_QUEUE = 'UserQueue'
RABBITMQ_USER_UPDATE_QUEUE = 'UserUpdateQueue'
RABBITMQ_USER_DELETE_QUEUE = 'UserDeleteQueue'
EXCHANGE_NAME = 'UserUpdateExchange'

# MSSQL connection parameters
MSSQL_CONN_STR = f"DRIVER={{FreeTDS}};SERVER={os.getenv('DB_SERVER')};DATABASE={os.getenv('WRITE_DB')};UID={os.getenv('DB_USER')};PWD={os.getenv('DB_PASSWORD')}"

# MongoDB connection parameters
MONGODB_URL = f"mongodb://{os.getenv('MONGOUSER')}:{os.getenv('MONGOPW')}{os.getenv('MONGOURL')}"
MONGODB_DB = os.getenv('MONGODB')
MONGODB_STORY_COLLECTION = 'stories'

# Connect to MongoDB
client = MongoClient(MONGODB_URL)
db = client[MONGODB_DB]
story_collection = db[MONGODB_STORY_COLLECTION]

# Connect to MSSQL
conn = pyodbc.connect(MSSQL_CONN_STR)
cursor = conn.cursor()

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue=RABBITMQ_NEW_USER_QUEUE, durable=True)
channel.queue_declare(queue=RABBITMQ_USER_UPDATE_QUEUE, durable=True)
channel.queue_declare(queue=RABBITMQ_USER_DELETE_QUEUE, durable=True)

# Declare the fanout exchange and bind the UserUpdateQueue to it
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout')
channel.queue_bind(exchange=EXCHANGE_NAME, queue=RABBITMQ_USER_UPDATE_QUEUE)
channel.queue_bind(exchange=EXCHANGE_NAME, queue=RABBITMQ_USER_DELETE_QUEUE)

# Define the callback function
def new_user_callback(ch, method, properties, body):
    print("Received:", body)

    # Prepare the user for insertion
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

def user_update_callback(ch, method, properties, body):
    print("Received:", body)

    # Prepare the user for insertion
    data = json.loads(body)
    user = data['user']
    userInfo = data['userInfo']

    # Get the user_id from the users table
    select_user = "SELECT user_id FROM users WHERE user_guid = ?"
    cursor.execute(select_user, user['guid'])
    user_id = cursor.fetchone()[0]
    created_at = parser.parse(userInfo['created_at'])

    # Insert the user info into MSSQL
    insert_user_info = "INSERT INTO user_info(first_name, last_name, email, created_at, user_id) VALUES (?,?,?,?,?)"
    cursor.execute(insert_user_info, userInfo['FirstName'], userInfo['LastName'], userInfo['Email'], created_at, user_id)

    conn.commit()

    print("User info updated in MSSQL")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)


def user_delete_callback(ch, method, properties, body):
    logging.info("Received: %s", body)

    # Prepare the users for deletion
    data = json.loads(body)

    # Iterate through the list inside the 'user_guid' key
    for user_guid in data['user_guid']:
        cursor = conn.cursor()
        try:
            # Convert user_guid to UUID
            userGuid = uuid.UUID(user_guid)  # No need to convert to string

            logging.info("Starting transaction for userGuid: %s", userGuid)

            # Delete the user from MSSQL
            delete_user = "DELETE FROM users WHERE user_guid = ?"
            logging.info("Executing delete query for userGuid: %s", userGuid)
            cursor.execute(delete_user, userGuid)

            # Commit the transaction
            logging.info("Committing transaction for userGuid: %s", userGuid)
            conn.commit()

            # Delete related stories from MongoDB
            logging.info("Deleting stories from MongoDB for userGuid: %s", userGuid)
            result = story_collection.delete_many({'userGuid': str(userGuid)})
            logging.info(f"Stories deleted from MongoDB for userGuid: {user_guid}, count: {result.deleted_count}")

            logging.info("User %s and related info deleted from MSSQL and MongoDB", user_guid)

        except (pyodbc.Error, ValueError) as e:
            # An error occurred, rollback the transaction
            logging.error("An error occurred: %s", e)
            logging.info("Rolling back transaction for userGuid: %s", userGuid)
            conn.rollback()
        finally:
            # Ensure the cursor is closed after each transaction
            cursor.close()

    # Acknowledge the message
    logging.info("Acknowledging message for delivery tag: %s", method.delivery_tag)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Start consuming messages
channel.basic_consume(queue=RABBITMQ_NEW_USER_QUEUE, on_message_callback=new_user_callback)
channel.basic_consume(queue=RABBITMQ_USER_UPDATE_QUEUE, on_message_callback=user_update_callback)
channel.basic_consume(queue=RABBITMQ_USER_DELETE_QUEUE, on_message_callback=user_delete_callback)

print('Waiting for new published content...')
channel.start_consuming()
