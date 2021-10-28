from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer, RollingPolicy,StreamingFileSink,OutputFileConfig
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, data_stream
from pyflink.table import StreamTableEnvironment
from pyflink.common.serialization import Encoder
from pyflink.datastream.execution_mode import RuntimeExecutionMode
import json

# Teste do job usando DataStream API
def job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(execution_mode=RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(1000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    t_env = StreamTableEnvironment.create(env)

    # deserialization_schema = JsonRowDeserializationSchema.builder().type_info(type_info=Types.ROW([]).build()
    deserialization_schema = SimpleStringSchema()
    kafka_consumer = FlinkKafkaConsumer(
        topics='A_RAIABD-TB_CANAL_VENDA',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': '10.1.165.35:9092,10.1.165.36:9092,10.1.165.37:9092',
        'group.id': 'test_group'}
        )

    ds = env.add_source(kafka_consumer)
    ds = ds.map(lambda x: json.loads(x)['payload']['after'] ,output_type=Types.ROW([Types.INT,Types.STRING]))
    # Saída
    t_env.execute_sql('''
                    CREATE TABLE sync (
                        cd_canal_venda int ,
                        ds_canal_venda STRING
                    ) WITH (
                        'connector' = 'filesystem',
                        'path' = 's3://kubernets-flink-poc/output/table/',
                        'format' = 'parquet'
                    )''')
    table = t_env.from_data_stream(ds)
    table.execute_insert("sync")

if __name__ == '__main__':
    job()
