from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.data_stream import WatermarkStrategy
from config.flink.flink_config import KAFKA_FLINK_Connector
from config.flink.flink_source import Source


def kafka_job(topic_name):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(KAFKA_FLINK_Connector)

    kafka_source = Source(topic_name=topic_name)

    input_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="KafkaSource",
        type_info=Types.STRING(),
    )
    input_stream.print()
    env.execute("Reading from a kafka stream")


if __name__ == "__main__":
    topic_name = "New-Topic"
    kafka_job(topic_name=topic_name)
