from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.data_stream import WatermarkStrategy

from pyflink.common import Configuration


def kafka_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///D:/SIA/Kafka-Flink/flink-sql-connector-kafka-3.2.0-1.18.jar")

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("New-Topic")
        .set_group_id("flink-consumer-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    input_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),  # Set appropriate watermark strategy if needed
        source_name="KafkaSource",
        type_info=Types.STRING(),
    )
    input_stream.print()
    env.execute("Reading from a kafka stream")


if __name__ == "__main__":
    kafka_job()
