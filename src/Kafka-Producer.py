# import sys
# from pathlib import Path
# sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.Kafka_config import create_kafka_topic, get_kafka_producer
from src.data_generator import fraudulent_data_generator, set_orders


def main():
    # load_dataset
    topic_name = "New-Topic"
    file_path = r"dataset\fraud-dataset.csv"
    fraudulent_dataset_generator = fraudulent_data_generator(file_path=file_path)
    producer = get_kafka_producer()

    # !create a new topic (if not already created)
    # create_kafka_topic(topic_name=topic_name,num_partitions=3,replication_factor=1)
    set_orders(
        producer=producer,
        topic_name=topic_name,
        data_generator=fraudulent_dataset_generator,
    )


if __name__ == "__main__":
    main()
