from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
import logging
import json


def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", client_id="GalalEwida-client"
    )
    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Error creating topic: {e}")


def get_kafka_producer():
    try:
        return KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: str(key).encode("utf-8"),
            acks="all",  #!sofy
            retries=5,  # !sofy
            max_in_flight_requests_per_connection=5,
            request_timeout_ms=10000,
            linger_ms=5,
            batch_size=32000,
        )
    except Exception as e:
        logging.error(f"Error creating Kafka producer: {e}")
        raise


def create_kafka_consumer(topic_name):
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            key_deserializer=lambda x: (x.decode("utf-8") if x else None),
        )
        return consumer
    except Exception as e:
        print(f"Error creating Kafka consumer: {e}")
        return None
