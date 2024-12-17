from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from config.flink.flink_config import FLINK_SERVER


def Source(topic_name):
    flink_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(FLINK_SERVER)
        .set_topics(topic_name)
        .set_group_id("flink-consumer-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return flink_source
