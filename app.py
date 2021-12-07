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
                    CREATE TABLE sink (
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
    t_env.execute_sql(""" INSERT INTO SINK SELECT
                                         CAST(f0 AS int ) as vl_iss
                                        ,CAST(f1 AS TIMESTAMP(3) ) as dt_fechto_credenciada
                                        ,CAST(f2 AS int ) as cd_credenciada
                                        ,CAST(f3 AS int ) as vl_subsidio_empresa
                                        ,CAST(f4 AS int ) as vl_pbm_reembolso
                                        ,CAST(f5 AS varchar ) as nr_cnpj_cgc
                                        ,CAST(f6 AS int ) as cd_operador_venda
                                        ,CAST(f7 AS int ) as cd_campanha
                                        ,CAST(f8 AS TIMESTAMP(3) ) as dt_atualizacao
                                        ,CAST(f9 AS TIMESTAMP(3) ) as dt_credenciada_fechto
                                        ,CAST(f10 AS int ) as cd_motivo_transferencia
                                        ,CAST(f11 AS int ) as cd_tp_cupom
                                        ,CAST(f12 AS int ) as nr_coo
                                        ,CAST(f13 AS varchar ) as nr_serie_nf
                                        ,CAST(f14 AS int ) as cd_nfe_tp_serie
                                        ,CAST(f15 AS varchar ) as cd_chave_acesso_nfe
                                        ,CAST(f16 AS varchar ) as cd_totalizador
                                        ,CAST(f17 AS int ) as vl_pis
                                        ,CAST(f18 AS int ) as vl_cofins
                                        ,CAST(f19 AS int ) as vl_base_pis
                                        ,CAST(f20 AS int ) as vl_base_cofins
                                        ,CAST(f21 AS int ) as cd_resposta_pesquisa
                                        ,CAST(f22 AS int ) as cd_tipo_transacao
                                        ,CAST(f23 AS int ) as vl_contabil
                                        ,CAST(f24 AS varchar ) as nr_trilha_cartao
                                        ,CAST(f25 AS int ) as vl_icms_desonerado_nf
                                        ,CAST(f26 AS int ) as vl_base_icms_fcp_nf
                                        ,CAST(f27 AS int ) as vl_base_icms_st_fcp_nf
                                        ,CAST(f28 AS int ) as vl_base_icms_st_fcp_ant_nf
                                        ,CAST(f29 AS int ) as vl_base_icms_st_gare_nf
                                        ,CAST(f30 AS int ) as vl_icms_fcp_nf
                                        ,CAST(f31 AS int ) as vl_icms_st_fcp_nf
                                        ,CAST(f32 AS int ) as vl_icms_st_fcp_ant_nf
                                        ,CAST(f33 AS int ) as vl_icms_st_gare_nf
                                        ,CAST(f34 AS int ) as vl_troco_nf
                                        ,CAST(f35 AS int ) as cd_tp_trib_cliente
                                        ,CAST(f36 AS int ) as cd_televenda
                                        ,CAST(f37 AS int ) as cd_tipo_pdv
                                        ,CAST(f38 AS int ) as cd_tp_registro
                                        ,CAST(f39 AS int ) as cd_filial
                                        ,CAST(f40 AS int ) as cd_transacao_cliente
                                        ,CAST(f41 AS int ) as cd_origem_nf
                                        ,CAST(f42 AS int ) as cd_forma_pagto
                                        ,CAST(f43 AS int ) as id_cliente
                                        ,CAST(f44 AS int ) as nr_seque
                                        ,CAST(f45 AS int ) as nr_cupom
                                        ,CAST(f46 AS int ) as nr_sequencia_cupom
                                        ,CAST(f47 AS int ) as nr_caixa
                                        ,CAST(f48 AS int ) as qt_pontos
                                        ,CAST(f49 AS int ) as vl_nf
                                        ,CAST(f50 AS int ) as st_transmissao
                                        ,CAST(f51 AS int ) as cd_ent_filant
                                        ,CAST(f52 AS int ) as nr_nf
                                        ,CAST(f53 AS int ) as vl_icms_nf
                                        ,CAST(f54 AS int ) as vl_ipi_nf
                                        ,CAST(f55 AS int ) as vl_frete
                                        ,CAST(f56 AS int ) as vl_seguro
                                        ,CAST(f57 AS int ) as vl_base_frete
                                        ,CAST(f58 AS int ) as vl_icms_frete
                                        ,CAST(f59 AS int ) as pc_icms_frete
                                        ,CAST(f60 AS int ) as vl_base_reduzida
                                        ,CAST(f61 AS int ) as vl_base_icms
                                        ,CAST(f62 AS int ) as vl_sinal_pontos
                                        ,CAST(f63 AS int ) as vl_sinal_brindes
                                        ,CAST(f64 AS int ) as vl_sinal_saldo
                                        ,CAST(f65 AS TIMESTAMP(3) ) as dt_fechto
                                        ,CAST(f66 AS TIMESTAMP(3) ) as dt_faturamento
                                        ,CAST(f67 AS int ) as vl_sinal_estoque
                                        ,CAST(f68 AS int ) as vl_nf_repasse
                                        ,CAST(f69 AS int ) as fl_venda_rg
                                        ,CAST(f70 AS int ) as vl_total_custo
                                        ,CAST(f71 AS int ) as vl_sinal_estoque_indisp_origem
                                        ,CAST(f72 AS TIMESTAMP(3) ) as dt_timestamp
                                        ,CAST(f73 AS int ) as nr_autorizacao
                                        ,CAST(f74 AS int ) as cd_tp_nf
                                        ,CAST(f75 AS int ) as cd_empresa_venda_vinculada
                                        ,CAST(f76 AS varchar ) as st_faturamento_convenio
                                        ,CAST(f77 AS int ) as cd_operador
                                        ,CAST(f78 AS int ) as st_transmissao_terceiros
                                        ,CAST(f79 AS int ) as nr_autorizacao_pbm
                                        ,CAST(f80 AS int ) as vl_glosa_pbm_repasse
                                        ,CAST(f81 AS int ) as vl_glosa_pbm_subsidio
                                        ,CAST(f82 AS int ) as vl_glosa_convenio
                                        ,CAST(f83 AS TIMESTAMP(3) ) as dt_glosa_convenio
                                        ,CAST(f84 AS TIMESTAMP(3) ) as dt_glosa_pbm_subsidio
                                        ,CAST(f85 AS TIMESTAMP(3) ) as dt_glosa_pbm_repasse
                                        ,CAST(f86 AS int ) as cd_pbr
                                        ,CAST(f87 AS int ) as vl_estorno_glosa_convenio
                                        ,CAST(f88 AS TIMESTAMP(3) ) as dt_estorno_glosa_convenio
                                        ,CAST(f89 AS int ) as cd_tipo_glosa
                                        ,CAST(f90 AS int ) as cd_tipo_glosa_pbmr
                                        ,CAST(f91 AS varchar ) as cds_chave_acesso_nfe
                                        ,CAST(f92 AS TIMESTAMP(3)) as dt_confirmacao_tracking
                                        ,CAST(f93 AS int ) as id_nf
                                        ,CAST(f94 AS int ) as cd_operacao_fiscal
                                        ,CAST(f95 AS int ) as cd_filial_origem
                                        ,CAST(f96 AS int ) as cd_filial_destino
                                        ,CAST(f97 AS TIMESTAMP(3) ) as dt_evento
                                        FROM InputTable
                                        """)
    t_env.execute('tb_nf')
if __name__ == '__main__':
    job()
