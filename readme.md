# REPO TEMPLATE

## Contributors

- Morten Bendeke
- Betül Iskender
- Yelong Hartl-He
- Zack Ottesen

## General Use

This is the subscriber for changes about users that is in RabbitMQ queues.<br>
It subscribes to queues regardning new or changed entities that need to be persisted in the write DB.<br>

## Environment Variables

Create a .env in the root folder.

- RABBITUSER=user
- RABBITPW=password
- RABBITURL=localhost
- DB_SERVER=localhost,1433
- WRITE_DB=writedb
- DB_USER=SA
- DB_PASSWORD=YourStrongPassword123

## How To Run

Make sure the environment variables are set.<br>

Lastly, use the following command:

```bash
python subscriber.py
```
