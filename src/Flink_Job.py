from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.data_stream import WatermarkStrategy
from config.flink.flink_config import KAFKA_FLINK_Connector
from config.flink.flink_source import Source, Sink
from config.flink.flinkmapfunction import BusinessRulesParser


def kafka_job(source_topic, destination_topic):
    # Set up the Stream Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(KAFKA_FLINK_Connector)

    # Set up the Kafka Source
    kafka_source = Source(topic_name=source_topic)

    # Create an input data stream from the Kafka source
    input_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="KafkaSource",
        type_info=Types.STRING(),
    )

    # Apply the BusinessRulesParser
    parsed_stream = input_stream.map(
        BusinessRulesParser(),
        output_type=Types.STRING(),
    )

    # Print parsed results (for debugging)
    parsed_stream.print()

    # Set up the Kafka Sink
    sink = Sink(topic_name=destination_topic)

    # Sink the parsed stream
    parsed_stream.sink_to(sink)

    # Execute the job
    env.execute(
        f"Kafka Job - Reading from {source_topic} and Writing to {destination_topic}"
    )


if __name__ == "__main__":
    source = "t1"
    destination = "t5"
    kafka_job(source_topic=source, destination_topic=destination)
