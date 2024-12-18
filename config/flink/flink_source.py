from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from config.flink.flink_config import Kafka_SERVER
from pyflink.datastream.connectors.kafka import DeliveryGuarantee


def Source(topic_name):
    flink_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(Kafka_SERVER)
        .set_topics(topic_name)
        .set_group_id("flink-consumer-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return flink_source


def Sink(topic_name):
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(Kafka_SERVER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic_name)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    return sink
