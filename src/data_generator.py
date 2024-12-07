import pandas as pd
import time
import logging


def fraudulent_data_generator(file_path):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        yield row.to_dict()


def set_orders(producer, topic_name, data_generator):

    try:
        for i, fraud_data in enumerate(data_generator):
            is_fraud = fraud_data["is-Fraud"]  # Determine fraud status
            partition = 0 if is_fraud == "No" else 1

            producer.send(
                topic_name, key=str(is_fraud), value=fraud_data, partition=partition
            )
            print(
                f"Produced record {i} to topic '{topic_name}' in partition {partition} with key '{is_fraud}'."
            )
            time.sleep(4)
    except Exception as e:
        logging.error(f"Error sending messages to Kafka: {e}")
    finally:
        producer.flush()
        producer.close()
