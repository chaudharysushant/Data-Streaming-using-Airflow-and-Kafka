import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for DAG
default_args = {
    'owner': 'admin',
    'start_date': datetime.now() - timedelta(minutes=1)
}

# Function to get random user data from API
def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/").json()
    return res['results'][0]

# Function to format API data into dictionary
def format_data(res):
    location = res['location']
    return {
        'id': str(uuid.uuid4()),
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}",
        'post_code': location['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }

# Function to stream data to Kafka
def stream_data():
    import json
    import logging
    from kafka import KafkaProducer, KafkaAdminClient
    from kafka.admin import NewTopic

    topic_name = "users"

    # Ensure topic exists
    try:
        admin = KafkaAdminClient(bootstrap_servers=['broker:29092'])
        existing_topics = admin.list_topics()
        if topic_name not in existing_topics:
            admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
        admin.close()
    except Exception as e:
        logging.warning(f"Topic check/create failed: {e}")

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        acks='all',
        retries=5,
        linger_ms=5
    )

    try:
        # Get and format data
        res = get_data()
        res = format_data(res)

        # Send data
        future = producer.send(topic_name, json.dumps(res).encode('utf-8'))
        future.get(timeout=20)  # Optional: wait for confirmation

        producer.flush()  # Ensure all messages are sent
        print(f"Message sent to {topic_name}: {res}")  # Terminal/log feedback
    except Exception as e:
        logging.error(f"Message send failed: {e}")
    finally:
        producer.close()

# Define DAG
with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval=timedelta(seconds=10),
    catchup=False
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
