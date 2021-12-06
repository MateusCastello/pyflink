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
    output_type=Types.ROW([Types.INT() ,Types.SQL_TIMESTAMP() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.STRING() ,Types.INT() ,Types.INT() ,Types.SQL_TIMESTAMP() ,Types.SQL_TIMESTAMP() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.STRING() ,Types.INT() ,Types.STRING() ,Types.STRING() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.STRING() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.SQL_TIMESTAMP() ,Types.SQL_TIMESTAMP() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.SQL_TIMESTAMP() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.STRING() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.SQL_TIMESTAMP() ,Types.SQL_TIMESTAMP() ,Types.SQL_TIMESTAMP() ,Types.INT() ,Types.INT() ,Types.SQL_TIMESTAMP() ,Types.INT() ,Types.INT() ,Types.STRING() ,Types.SQL_TIMESTAMP() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.INT() ,Types.SQL_TIMESTAMP()]))
    # Saída
    t_env.execute_sql('''
                    CREATE TABLE SINK (
                                        vl_iss int ,
                                        dt_fechto_credenciada TIMESTAMP(3),
                                        cd_credenciada int ,
                                        vl_subsidio_empresa int ,
                                        vl_pbm_reembolso int ,
                                        nr_cnpj_cgc varchar ,
                                        cd_operador_venda int ,
                                        cd_campanha int ,
                                        dt_atualizacao TIMESTAMP(3) ,
                                        dt_credenciada_fechto TIMESTAMP(3) ,
                                        cd_motivo_transferencia int ,
                                        cd_tp_cupom int ,
                                        nr_coo int ,
                                        nr_serie_nf varchar ,
                                        cd_nfe_tp_serie int ,
                                        cd_chave_acesso_nfe varchar ,
                                        cd_totalizador varchar ,
                                        vl_pis int ,
                                        vl_cofins int ,
                                        vl_base_pis int ,
                                        vl_base_cofins int ,
                                        cd_resposta_pesquisa int ,
                                        cd_tipo_transacao int ,
                                        vl_contabil int ,
                                        nr_trilha_cartao varchar ,
                                        vl_icms_desonerado_nf int ,
                                        vl_base_icms_fcp_nf int ,
                                        vl_base_icms_st_fcp_nf int ,
                                        vl_base_icms_st_fcp_ant_nf int ,
                                        vl_base_icms_st_gare_nf int ,
                                        vl_icms_fcp_nf int ,
                                        vl_icms_st_fcp_nf int ,
                                        vl_icms_st_fcp_ant_nf int ,
                                        vl_icms_st_gare_nf int ,
                                        vl_troco_nf int ,
                                        cd_tp_trib_cliente int ,
                                        cd_televenda int ,
                                        cd_tipo_pdv int ,
                                        cd_tp_registro int ,
                                        cd_filial int ,
                                        cd_transacao_cliente int ,
                                        cd_origem_nf int ,
                                        cd_forma_pagto int ,
                                        id_cliente int ,
                                        nr_seque int ,
                                        nr_cupom int ,
                                        nr_sequencia_cupom int ,
                                        nr_caixa int ,
                                        qt_pontos int ,
                                        vl_nf int ,
                                        st_transmissao int ,
                                        cd_ent_filant int ,
                                        nr_nf int ,
                                        vl_icms_nf int ,
                                        vl_ipi_nf int ,
                                        vl_frete int ,
                                        vl_seguro int ,
                                        vl_base_frete int ,
                                        vl_icms_frete int ,
                                        pc_icms_frete int ,
                                        vl_base_reduzida int ,
                                        vl_base_icms int ,
                                        vl_sinal_pontos int ,
                                        vl_sinal_brindes int ,
                                        vl_sinal_saldo int ,
                                        dt_fechto TIMESTAMP(3) ,
                                        dt_faturamento TIMESTAMP(3) ,
                                        vl_sinal_estoque int ,
                                        vl_nf_repasse int ,
                                        fl_venda_rg int ,
                                        vl_total_custo int ,
                                        vl_sinal_estoque_indisp_origem int ,
                                        dt_timestamp TIMESTAMP(3) ,
                                        nr_autorizacao int ,
                                        cd_tp_nf int ,
                                        cd_empresa_venda_vinculada int ,
                                        st_faturamento_convenio varchar ,
                                        cd_operador int ,
                                        st_transmissao_terceiros int ,
                                        nr_autorizacao_pbm int ,
                                        vl_glosa_pbm_repasse int ,
                                        vl_glosa_pbm_subsidio int ,
                                        vl_glosa_convenio int ,
                                        dt_glosa_convenio TIMESTAMP(3) ,
                                        dt_glosa_pbm_subsidio TIMESTAMP(3) ,
                                        dt_glosa_pbm_repasse TIMESTAMP(3) ,
                                        cd_pbr int ,
                                        vl_estorno_glosa_convenio int ,
                                        dt_estorno_glosa_convenio TIMESTAMP(3) ,
                                        cd_tipo_glosa int ,
                                        cd_tipo_glosa_pbmr int ,
                                        cds_chave_acesso_nfe varchar ,
                                        dt_confirmacao_tracking TIMESTAMP(3) ,
                                        id_nf int ,
                                        cd_operacao_fiscal int ,
                                        cd_filial_origem int ,
                                        cd_filial_destino int ,
                                        dt_evento TIMESTAMP(3)
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
    t_env.execute('tb_nf')

if __name__ == '__main__':
    job()
