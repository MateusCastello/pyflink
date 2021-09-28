import os
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka, Elasticsearch
from pyflink.table.window import Tumble

def transactions_source(st_env):
    create_kafka_source_ddl = """
            CREATE TABLE source(
                cd_canal_venda bigint,
                ds_canal_venda string
            ) WITH
            (
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = 'A_RAIABD-TB_CANAL_VENDA',
              'connector.properties.bootstrap.servers' = '10.1.165.35:9092,10.1.165.36:9092,10.1.165.37:9092',
              'connector.properties.group.id' = 'test_3',
              'connector.properties.client.id' = '1',
              'connector.startup-mode' = 'latest-offset',
              'format.type' = 'debezium-json',
              'debezium-json.schema-include' = 'true'
            )
            """
    st_env.execute_sql(create_kafka_source_ddl)

def register_transactions_sink_into_csv(st_env):
    result_file = "s3://rd-datalake-dev-temp/spark_dev/flink/out.csv"
    st_env.register_table_sink("sink_into_csv",
    CsvTableSink(
        ["cd_canal_venda",
        "ds_canal_venda"],
        [DataTypes.BIGINT(),
        DataTypes.STRING()],
        result_file)
        )
def main():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment.create(stream_execution_environment=s_env)

    transactions_source(st_env)
    register_transactions_sink_into_csv(st_env)

    st_env.from_path("source")\
        .select("*")\
        .execute_insert("sink_into_csv")
    st_env.execute()

if __name__ == '__main__':
    main()
