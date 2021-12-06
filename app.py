from pyflink import table
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer, JdbcConnectionOptions, JdbcSink
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.execution_mode import RuntimeExecutionMode
import json

# Teste do job usando DataStream API

def job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(execution_mode=RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(5000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    t_env = StreamTableEnvironment.create(env)

    # deserialization_schema = JsonRowDeserializationSchema.builder().type_info(type_info=Types.ROW([]).build()
    deserialization_schema = SimpleStringSchema()
    kafka_consumer = FlinkKafkaConsumer(
        topics='TB_NF',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'kafka-service:9092',
                    'group.id': 'test_group',
                    'startup-mode': 'earliest-offset'}
    )

    ds = env.add_source(kafka_consumer)
    ds = ds.map(lambda x: list(json.loads(x)['payload']['after'].values()),
                output_type=Types.ROW([Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.STRING()]))

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
    t_env.execute_sql("""INSERT INTO SINK SELECT
                                        'f0' AS VL_ISS ,
                                         CAST('f1' AS TIMESTAMP(3)) AS DT_FECHTO_CREDENCIADA,
                                        'f2' AS CD_CREDENCIADA ,
                                        'f3' AS VL_SUBSIDIO_EMPRESA ,
                                        'f4' AS VL_PBM_REEMBOLSO ,
                                        'f5' AS NR_CNPJ_CGC ,
                                        'f6' AS CD_OPERADOR_VENDA ,
                                        'f7' AS CD_CAMPANHA ,
                                         CAST('f8' AS TIMESTAMP(3)) AS DT_ATUALIZACAO,
                                         CAST('f9' AS TIMESTAMP(3)) AS DT_CREDENCIADA_FECHTO ,
                                        'f10' AS CD_MOTIVO_TRANSFERENCIA ,
                                        'f11' AS CD_TP_CUPOM ,
                                        'f12' AS NR_COO ,
                                        'f13' AS NR_SERIE_NF ,
                                        'f14' AS CD_NFE_TP_SERIE ,
                                        'f15' AS CD_CHAVE_ACESSO_NFE ,
                                        'f16' AS CD_TOTALIZADOR ,
                                        'f17' AS VL_PIS ,
                                        'f18' AS VL_COFINS ,
                                        'f19' AS VL_BASE_PIS ,
                                        'f20' AS VL_BASE_COFINS ,
                                        'f21' AS CD_RESPOSTA_PESQUISA ,
                                        'f22' AS CD_TIPO_TRANSACAO ,
                                        'f23' AS VL_CONTABIL ,
                                        'f24' AS NR_TRILHA_CARTAO ,
                                        'f25' AS VL_ICMS_DESONERADO_NF ,
                                        'f26' AS VL_BASE_ICMS_FCP_NF ,
                                        'f27' AS VL_BASE_ICMS_ST_FCP_NF ,
                                        'f28' AS VL_BASE_ICMS_ST_FCP_ANT_NF ,
                                        'f29' AS VL_BASE_ICMS_ST_GARE_NF ,
                                        'f30' AS VL_ICMS_FCP_NF ,
                                        'f31' AS VL_ICMS_ST_FCP_NF ,
                                        'f32' AS VL_ICMS_ST_FCP_ANT_NF ,
                                        'f33' AS VL_ICMS_ST_GARE_NF ,
                                        'f34' AS VL_TROCO_NF ,
                                        'f35' AS CD_TP_TRIB_CLIENTE ,
                                        'f36' AS CD_TELEVENDA ,
                                        'f37' AS CD_TIPO_PDV ,
                                        'f38' AS  CD_TP_REGISTRO,
                                        'f39' AS CD_FILIAL ,
                                        'f40' AS CD_TRANSACAO_CLIENTE ,
                                        'f41' AS CD_ORIGEM_NF ,
                                        'f42' AS CD_FORMA_PAGTO ,
                                        'f43' AS ID_CLIENTE ,
                                        'f44' AS NR_SEQUE ,
                                        'f45' AS NR_CUPOM ,
                                        'f46' AS NR_SEQUENCIA_CUPOM ,
                                        'f47' AS NR_CAIXA ,
                                        'f48' AS QT_PONTOS ,
                                        'f49' AS VL_NF ,
                                        'f50' AS ST_TRANSMISSAO ,
                                        'f51' AS CD_ENT_FILANT ,
                                        'f52' AS NR_NF ,
                                        'f53' AS VL_ICMS_NF ,
                                        'f54' AS VL_IPI_NF ,
                                        'f55' AS VL_FRETE ,
                                        'f56' AS VL_SEGURO ,
                                        'f57' AS VL_BASE_FRETE ,
                                        'f58' AS VL_ICMS_FRETE ,
                                        'f59' AS PC_ICMS_FRETE ,
                                        'f60' AS VL_BASE_REDUZIDA ,
                                        'f61' AS VL_BASE_ICMS ,
                                        'f62' AS VL_SINAL_PONTOS ,
                                        'f63' AS VL_SINAL_BRINDES ,
                                        'f64' AS VL_SINAL_SALDO ,
                                         CAST('f65' AS TIMESTAMP(3)) AS DT_FECHTO ,
                                         CAST('f66' AS TIMESTAMP(3)) AS DT_FATURAMENTO,
                                        'f67' AS VL_SINAL_ESTOQUE ,
                                        'f68' AS VL_NF_REPASSE ,
                                        'f69' AS FL_VENDA_RG ,
                                        'f70' AS VL_TOTAL_CUSTO ,
                                        'f71' AS VL_SINAL_ESTOQUE_INDISP_ORIGEM ,
                                         CAST('f72' AS TIMESTAMP(3)) AS DT_TIMESTAMP,
                                        'f73' AS NR_AUTORIZACAO ,
                                        'f74' AS CD_TP_NF ,
                                        'f75' AS CD_EMPRESA_VENDA_VINCULADA ,
                                        'f76' AS ST_FATURAMENTO_CONVENIO ,
                                        'f77' AS CD_OPERADOR ,
                                        'f78' AS ST_TRANSMISSAO_TERCEIROS ,
                                        'f79' AS NR_AUTORIZACAO_PBM ,
                                        'f80' AS VL_GLOSA_PBM_REPASSE ,
                                        'f81' AS VL_GLOSA_PBM_SUBSIDIO ,
                                        'f82' AS VL_GLOSA_CONVENIO ,
                                        CAST('f83' AS TIMESTAMP(3)) AS DT_GLOSA_CONVENIO ,
                                        CAST('f84' AS TIMESTAMP(3)) AS DT_GLOSA_PBM_SUBSIDIO ,
                                        CAST('f85' AS TIMESTAMP(3)) AS DT_GLOSA_PBM_REPASSE ,
                                        'f86' AS CD_PBR ,
                                        'f87' AS VL_ESTORNO_GLOSA_CONVENIO ,
                                        CAST('f88' AS TIMESTAMP(3)) AS DT_ESTORNO_GLOSA_CONVENIO ,
                                        'f89' AS CD_TIPO_GLOSA ,
                                        'f90' AS CD_TIPO_GLOSA_PBMR ,
                                        'f91' AS CDS_CHAVE_ACESSO_NFE ,
                                        CAST('f92' AS TIMESTAMP(3)) AS DT_CONFIRMACAO_TRACKING,
                                        'f93' AS ID_NF ,
                                        'f94' AS CD_OPERACAO_FISCAL ,
                                        'f95' AS CD_FILIAL_ORIGEM ,
                                        'f96' AS CD_FILIAL_DESTINO ,
                                        CAST('f97' AS TIMESTAMP(3)) AS DT_EVENTO
                                        FROM InputTable
                                        """)

    t_env.execute('tb_nf')

if __name__ == '__main__':
    job()
