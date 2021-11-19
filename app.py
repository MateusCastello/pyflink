from pyflink.datastream.connectors import FlinkKafkaConsumer, RollingPolicy,StreamingFileSink,OutputFileConfig
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import Encoder
from pyflink.datastream.execution_mode import RuntimeExecutionMode

# Teste do job usando DataStream API
def job():
    env = StreamExecutionEnvironment.get_execution_environment()
    #env.set_runtime_mode(execution_mode=RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(1000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    deserialization_schema = SimpleStringSchema()
    kafka_consumer = FlinkKafkaConsumer(
        topics='transactions-data',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'kafka:29092',
        'group.id': 'test_group69',
        'auto.offset.reset':'earliest'})
    ds = env.add_source(kafka_consumer)


    # Saída
    output_path = 's3://rd-datalake-dev-temp/spark_dev/flink/output/'
    file_sink = StreamingFileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder()
        .with_part_suffix(".out")
        .build()) \
        .with_rolling_policy(RollingPolicy \
            .default_rolling_policy(part_size=5*1024*1024,rollover_interval=10*1000,inactivity_interval=10*1000)) \
                .build()
    ds.add_sink(file_sink)
    # emit ds to print sink
    env.execute("tb_nf")

if __name__ == '__main__':
    job()
