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
                    'startup-mode': 'earliest-offset'}
    )

    ds = env.add_source(kafka_consumer)
    ds = ds.map(lambda x: list(json.loads(x)['payload']['after'].values()),
                output_type=Types.ROW([Types.INT(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP()]))
    # Saída
    jdbc_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_user_name("mantapostgres") \
        .with_password("postgres_password") \
        .with_driver_name("org.postgresql.Driver")\
        .with_url("jdbc:postgresql://mantabase.c0uugfnq0yzw.us-east-1.rds.amazonaws.com:5432/mantabase") \
        .build()

    ds.add_sink(JdbcSink.sink("insert into public.sink(vl_iss,dt_fechto_credenciada,cd_credenciada,vl_subsidio_empresa,vl_pbm_reembolso,nr_cnpj_cgc,cd_operador_venda,cd_campanha,dt_atualizacao,dt_credenciada_fechto,cd_motivo_transferencia,cd_tp_cupom,nr_coo,nr_serie_nf,cd_nfe_tp_serie,cd_chave_acesso_nfe,cd_totalizador,vl_pis,vl_cofins,vl_base_pis,vl_base_cofins,cd_resposta_pesquisa,cd_tipo_transacao,vl_contabil,nr_trilha_cartao,vl_icms_desonerado_nf,vl_base_icms_fcp_nf,vl_base_icms_st_fcp_nf,vl_base_icms_st_fcp_ant_nf,vl_base_icms_st_gare_nf,vl_icms_fcp_nf,vl_icms_st_fcp_nf,vl_icms_st_fcp_ant_nf,vl_icms_st_gare_nf,vl_troco_nf,cd_tp_trib_cliente,cd_televenda,cd_tipo_pdv,cd_tp_registro,cd_filial,cd_transacao_cliente,cd_origem_nf,cd_forma_pagto,id_cliente,nr_seque,nr_cupom,nr_sequencia_cupom,nr_caixa,qt_pontos,vl_nf,st_transmissao,cd_ent_filant,nr_nf,vl_icms_nf,vl_ipi_nf,vl_frete,vl_seguro,vl_base_frete,vl_icms_frete,pc_icms_frete,vl_base_reduzida,vl_base_icms,vl_sinal_pontos,vl_sinal_brindes,vl_sinal_saldo,dt_fechto,dt_faturamento,vl_sinal_estoque,vl_nf_repasse,fl_venda_rg,vl_total_custo,vl_sinal_estoque_indisp_origem,dt_timestamp,nr_autorizacao,cd_tp_nf,cd_empresa_venda_vinculada,st_faturamento_convenio,cd_operador,st_transmissao_terceiros,nr_autorizacao_pbm,vl_glosa_pbm_repasse,vl_glosa_pbm_subsidio,vl_glosa_convenio,dt_glosa_convenio,dt_glosa_pbm_subsidio,dt_glosa_pbm_repasse,cd_pbr,vl_estorno_glosa_convenio,dt_estorno_glosa_convenio,cd_tipo_glosa,cd_tipo_glosa_pbmr,cds_chave_acesso_nfe,dt_confirmacao_tracking,id_nf,cd_operacao_fiscal,cd_filial_origem,cd_filial_destino,dt_evento) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",type_info=Types.ROW([Types.INT(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.SQL_TIMESTAMP()]),jdbc_connection_options=jdbc_options))

    #table = t_env.from_data_stream(ds)
    #t_env.create_temporary_view("InputTable", table)
    #t_env.execute_sql("INSERT INTO SINK SELECT * FROM InputTable")
    # table.execute_insert("SINK").wait()
    env.execute('tb_nf')


if __name__ == '__main__':
    job()
