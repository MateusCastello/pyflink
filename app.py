from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
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
        topics='TB_NF',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'kafka-service:9092',
        'group.id': 'test_group',
        'startup-mode':'earliest-offset'}
        )

    ds = env.add_source(kafka_consumer)
    ds = ds.map(lambda x: list(json.loads(x)['payload']['after'].values()),
    output_type=Types.ROW([Types.INT(),Types.STRING(),Types.INT(),Types.INT(),Types.INT(),Types.STRING(),Types.INT(),Types.INT(),Types.STRING(),Types.STRING(),Types.INT(),Types.INT(),Types.INT(),Types.STRING(),Types.INT(),Types.STRING(),Types.STRING(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.STRING(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.STRING(),Types.STRING(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.STRING(),Types.INT(),Types.INT(),Types.INT(),Types.STRING(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.STRING(),Types.STRING(),Types.STRING(),Types.INT(),Types.INT(),Types.STRING(),Types.INT(),Types.INT(),Types.STRING(),Types.STRING(),Types.INT(),Types.INT(),Types.INT(),Types.INT(),Types.STRING()]))
    # Saída
    t_env.execute_sql('''
                    CREATE TABLE SINK (
                                        VL_ISS INT,
                                        DT_FECHTO_CREDENCIADA STRING,
                                        CD_CREDENCIADA INT,
                                        VL_SUBSIDIO_EMPRESA INT,
                                        VL_PBM_REEMBOLSO INT,
                                        NR_CNPJ_CGC STRING,
                                        CD_OPERADOR_VENDA INT,
                                        CD_CAMPANHA INT,
                                        DT_ATUALIZACAO STRING,
                                        DT_CREDENCIADA_FECHTO STRING,
                                        CD_MOTIVO_TRANSFERENCIA INT,
                                        CD_TP_CUPOM INT,
                                        NR_COO INT,
                                        NR_SERIE_NF STRING,
                                        CD_NFE_TP_SERIE INT,
                                        CD_CHAVE_ACESSO_NFE STRING,
                                        CD_TOTALIZADOR STRING,
                                        VL_PIS INT,
                                        VL_COFINS INT,
                                        VL_BASE_PIS INT,
                                        VL_BASE_COFINS INT,
                                        CD_RESPOSTA_PESQUISA INT,
                                        CD_TIPO_TRANSACAO INT,
                                        VL_CONTABIL INT,
                                        NR_TRILHA_CARTAO STRING,
                                        VL_ICMS_DESONERADO_NF INT,
                                        VL_BASE_ICMS_FCP_NF INT,
                                        VL_BASE_ICMS_ST_FCP_NF INT,
                                        VL_BASE_ICMS_ST_FCP_ANT_NF INT,
                                        VL_BASE_ICMS_ST_GARE_NF INT,
                                        VL_ICMS_FCP_NF INT,
                                        VL_ICMS_ST_FCP_NF INT,
                                        VL_ICMS_ST_FCP_ANT_NF INT,
                                        VL_ICMS_ST_GARE_NF INT,
                                        VL_TROCO_NF INT,
                                        CD_TP_TRIB_CLIENTE INT,
                                        CD_TELEVENDA INT,
                                        CD_TIPO_PDV INT,
                                        CD_TP_REGISTRO INT,
                                        CD_FILIAL INT,
                                        CD_TRANSACAO_CLIENTE INT,
                                        CD_ORIGEM_NF INT,
                                        CD_FORMA_PAGTO INT,
                                        ID_CLIENTE INT,
                                        NR_SEQUE INT,
                                        NR_CUPOM INT,
                                        NR_SEQUENCIA_CUPOM INT,
                                        NR_CAIXA INT,
                                        QT_PONTOS INT,
                                        VL_NF INT,
                                        ST_TRANSMISSAO INT,
                                        CD_ENT_FILANT INT,
                                        NR_NF INT,
                                        VL_ICMS_NF INT,
                                        VL_IPI_NF INT,
                                        VL_FRETE INT,
                                        VL_SEGURO INT,
                                        VL_BASE_FRETE INT,
                                        VL_ICMS_FRETE INT,
                                        PC_ICMS_FRETE INT,
                                        VL_BASE_REDUZIDA INT,
                                        VL_BASE_ICMS INT,
                                        VL_SINAL_PONTOS INT,
                                        VL_SINAL_BRINDES INT,
                                        VL_SINAL_SALDO INT,
                                        DT_FECHTO STRING,
                                        DT_FATURAMENTO STRING,
                                        VL_SINAL_ESTOQUE INT,
                                        VL_NF_REPASSE INT,
                                        FL_VENDA_RG INT,
                                        VL_TOTAL_CUSTO INT,
                                        VL_SINAL_ESTOQUE_INDISP_ORIGEM INT,
                                        DT_TIMESTAMP STRING,
                                        NR_AUTORIZACAO INT,
                                        CD_TP_NF INT,
                                        CD_EMPRESA_VENDA_VINCULADA INT,
                                        ST_FATURAMENTO_CONVENIO STRING,
                                        CD_OPERADOR INT,
                                        ST_TRANSMISSAO_TERCEIROS INT,
                                        NR_AUTORIZACAO_PBM INT,
                                        VL_GLOSA_PBM_REPASSE INT,
                                        VL_GLOSA_PBM_SUBSIDIO INT,
                                        VL_GLOSA_CONVENIO INT,
                                        DT_GLOSA_CONVENIO STRING,
                                        DT_GLOSA_PBM_SUBSIDIO STRING,
                                        DT_GLOSA_PBM_REPASSE STRING,
                                        CD_PBR INT,
                                        VL_ESTORNO_GLOSA_CONVENIO INT,
                                        DT_ESTORNO_GLOSA_CONVENIO STRING,
                                        CD_TIPO_GLOSA INT,
                                        CD_TIPO_GLOSA_PBMR INT,
                                        CDS_CHAVE_ACESSO_NFE STRING,
                                        DT_CONFIRMACAO_TRACKING STRING,
                                        ID_NF INT,
                                        CD_OPERACAO_FISCAL INT,
                                        CD_FILIAL_ORIGEM INT,
                                        CD_FILIAL_DESTINO INT,
                                        DT_EVENTO STRING
                                    )
                WITH (
                        'connector.type' = 'jdbc',
                        'connector.url' = 'jdbc:postgresql://mantabase.c0uugfnq0yzw.us-east-1.rds.amazonaws.com:5432/mantabase',
                        'connector.table' = 'public.sink',
                        'connector.username' = 'mantapostgres',
                        'connector.password' = 'postgres_password',
                        'connector.write.flush.interval' = '1s'
                    )''')
    table = t_env.from_data_stream(ds)
    t_env.create_temporary_view("InputTable", table)
    t_env.execute_sql("INSERT INTO SINK SELECT * FROM InputTable")
    t_env.execute("tb_nf")

if __name__ == '__main__':
    job()
