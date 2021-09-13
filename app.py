import os
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka, Elasticsearch
from pyflink.table.window import Tumble

def transactions_source(st_env):
    create_kafka_source_ddl = """
            CREATE TABLE source(
                customer VARCHAR,
                transaction_type VARCHAR,
                online_payment_amount DOUBLE,
                in_store_payment_amount DOUBLE,
                lat DOUBLE,
                lon DOUBLE,
                transaction_datetime TIMESTAMP(3)
            ) WITH (
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = 'transactions-data',
              'connector.properties.bootstrap.servers' = 'kafka:29092',
              'connector.properties.group.id' = 'test_3',
              'connector.properties.client.id' = '1',
              'connector.startup-mode' = 'latest-offset',
              'format.type' = 'json'
            )
            """
    st_env.execute_sql(create_kafka_source_ddl)


def register_transactions_source(st_env):
    st_env.connect(Kafka()
                   .version("universal")
                   .topic("transactions-data")
                   .start_from_latest()
                   .property("zookeeper.connect", "localhost:2181")
                   .property("group.id","teste")
                   .property("clientId","1")
                   .property("bootstrap.servers", "kafka:29092")) \
        .with_format(Json()
        .fail_on_missing_field(True)
        .schema(DataTypes.ROW([
        DataTypes.FIELD("customer", DataTypes.STRING()),
        DataTypes.FIELD("transaction_type", DataTypes.STRING()),
        DataTypes.FIELD("online_payment_amount", DataTypes.DOUBLE()),
        DataTypes.FIELD("in_store_payment_amount", DataTypes.DOUBLE()),
        DataTypes.FIELD("lat", DataTypes.DOUBLE()),
        DataTypes.FIELD("lon", DataTypes.DOUBLE()),
        DataTypes.FIELD("transaction_datetime", DataTypes.TIMESTAMP(3))]))) \
        .with_schema(Schema()
        .field("customer", DataTypes.STRING())
        .field("transaction_type", DataTypes.STRING())
        .field("online_payment_amount", DataTypes.DOUBLE())
        .field("in_store_payment_amount", DataTypes.DOUBLE())
        .field("lat", DataTypes.DOUBLE())
        .field("lon", DataTypes.DOUBLE())
        .field("rowtime", DataTypes.TIMESTAMP(3))
        .rowtime(
        Rowtime()
            .timestamps_from_field("transaction_datetime")
            .watermarks_periodic_bounded(60000))) \
        .in_append_mode() \
        .register_table_source("source")

def register_transactions_sink_into_csv(st_env):
    result_file = "/opt/examples/data/output/output_file.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    st_env.register_table_sink("sink_into_csv",
                               CsvTableSink(["customer",
                                             "count_transactions",
                                             "total_online_payment_amount",
                                             "total_in_store_payment_amount",
                                             "lat",
                                             "lon",
                                             "transaction_datetime"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING(),
                                             DataTypes.DOUBLE(),
                                             DataTypes.DOUBLE(),
                                             DataTypes.DOUBLE(),
                                             DataTypes.DOUBLE(),
                                             DataTypes.TIMESTAMP(3)],
                                            result_file))
def transactions_job():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment \
        .create(stream_execution_environment=s_env)
    st_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

    #register_transactions_source(st_env)
    transactions_source(st_env)
    register_transactions_sink_into_csv(st_env)

    st_env.from_path("source")\
        .select("customer,transaction_type,online_payment_amount,in_store_payment_amount,lat,lon,transaction_datetime")\
        .insert_into("sink_into_csv")
    st_env.execute("app")


if __name__ == '__main__':
    transactions_job()

