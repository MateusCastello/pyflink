from pyflink.datastream.connectors import FlinkKafkaConsumer, RollingPolicy,StreamingFileSink,OutputFileConfig
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import Encoder
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment


# Teste do job usando DataStream API
def job():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    env.set_runtime_mode(execution_mode=RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(1000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    deserialization_schema = SimpleStringSchema()
    kafka_consumer = FlinkKafkaConsumer(
        topics='A_RAIABD-TB_CANAL_VENDA',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': '10.1.165.35:9092,10.1.165.36:9092,10.1.165.37:9092',
        'group.id': 'test_group'})
    ds = env.add_source(kafka_consumer)


    # Saída
    output_path = 's3://rd-datalake-dev-temp/spark_dev/flink/output/'
    #file_sink = StreamingFileSink \
    #    .for_row_format(output_path, Encoder.simple_string_encoder()) \
    #    .with_output_file_config(OutputFileConfig.builder()
    #    .with_part_suffix(".out")
    #    .build()) \
    #    .with_rolling_policy(RollingPolicy \
    #        .default_rolling_policy(part_size=5*1024*1024,rollover_interval=10*1000,inactivity_interval=10*1000)) \
    #            .build()
    #ds.add_sink(file_sink)
    # emit ds to print sink

    # Converts the datastream to table, to write in parquet format
    table = t_env.from_data_stream(ds, 'a')
    t_env.execute_sql(f"""
        CREATE TABLE my_sink (
          b VARCHAR
        ) WITH (
          'connector' = 'filesystem'
          'path' = {output_path},
          'format' = 'parquet'
        )
    """)
    table.execute_insert("my_sink")
    env.execute("tb_canal_venda")

if __name__ == '__main__':
    job()
