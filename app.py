import os
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka, Elasticsearch
from pyflink.table.window import Tumble

def main():
    input = """
            CREATE TABLE source(
                aa_validade_cartao_credito bigint, 
                cd_banco_cheque bigint, 
                cd_bandeira_cartao bigint, 
                cd_canal_internet string, 
                cd_carrier bigint, 
                cd_cmc7_cheque string, 
                cd_entregador bigint, 
                cd_filial bigint, 
                cd_filial_origem_pedido bigint, 
                cd_forma_pagto bigint, 
                cd_janela_apoio bigint, 
                cd_janela_pdv bigint, 
                cd_motivo_abandono_televenda bigint, 
                cd_motivo_televenda bigint, 
                cd_operador bigint, 
                cd_operador_baixa bigint, 
                cd_operador_fatura string, 
                cd_operador_modalidade string, 
                cd_operador_sangria string, 
                cd_origem_televenda bigint, 
                cd_status_televenda bigint, 
                cd_televenda bigint, 
                cd_televenda_pai bigint, 
                cd_televenda_turno string, 
                cd_tipo_cartao bigint, 
                cd_tipo_trans_cartao bigint, 
                ds_nsu string, 
                ds_nsu_cancelamento string, 
                ds_obs string, 
                ds_obs_dif_baixa string, 
                dthr_pedido timestamp(3), 
                dt_cheque timestamp(3), 
                dt_envio_superpolo timestamp(3), 
                dt_fatura timestamp(3), 
                dt_pedido timestamp(3), 
                dt_sangria timestamp(3), 
                dt_transacao_tef timestamp(3), 
                fl_acao_judicial bigint, 
                fl_agenda bigint, 
                fl_aguardando_baixa bigint, 
                fl_aguardando_ped_venda bigint, 
                fl_convenio bigint, 
                fl_devolucao bigint, 
                fl_emergencial bigint, 
                fl_endereco_novo bigint, 
                fl_escaneado bigint, 
                fl_frete_liberado bigint, 
                fl_imprimir_offline string, 
                fl_manutencao bigint, 
                fl_orcamento bigint, 
                hr_atend_previsto timestamp(3), 
                hr_entrega timestamp(3), 
                hr_fatura timestamp(3), 
                hr_impressao timestamp(3), 
                hr_retorno_entrega timestamp(3), 
                hr_saida_entrega timestamp(3), 
                id_cliente bigint, 
                ip_operador string, 
                md_duracao_atendimento_s bigint, 
                mm_validade_cartao_credito bigint, 
                nm_recebeu string, 
                nr_agencia bigint, 
                nr_autorizacao_pbm bigint, 
                nr_cartao_credito string, 
                nr_cep string, 
                nr_cheque bigint, 
                nr_codigo_seguranca string, 
                nr_conta_corrente string, 
                nr_cpf_cheque string, 
                nr_cpf_cnpj_nota string, 
                nr_cupom bigint, 
                nr_dependente_idade bigint, 
                nr_endereco bigint, 
                nr_objeto string, 
                nr_pedido bigint, 
                nr_pedido_devolucao string, 
                nr_relatorio_sangria string, 
                nr_rg_recebeu string, 
                nr_romaneio_entrega bigint, 
                nr_rot_dist bigint, 
                nr_sequencia_cupom bigint, 
                nr_seq_pedido bigint, 
                pc_desc decimal(38,18), 
                qt_parcelas bigint, 
                qt_pontos bigint, 
                qt_roteirizacao bigint, 
                qt_total bigint, 
                qt_total_medicamento bigint, 
                qt_total_perfumaria bigint, 
                st_retira_caixa string, 
                vl_abatimento decimal(38,18), 
                vl_abatimento_vista decimal(38,18), 
                vl_arredondamento decimal(38,18), 
                vl_custo_frete decimal(38,18), 
                vl_desc_cartao decimal(38,18), 
                vl_desc_faixa1 decimal(38,18), 
                vl_desc_oferta decimal(38,18), 
                vl_diferenca_baixa decimal(38,18), 
                vl_diferenca_devol_cliente decimal(38,18), 
                vl_dinheiro decimal(38,18), 
                vl_frete decimal(38,18), 
                vl_gorjeta decimal(38,18), 
                vl_nf_devolucao decimal(38,18), 
                vl_repasse decimal(38,18), 
                vl_subsidio_empresa decimal(38,18), 
                vl_total decimal(38,18), 
                vl_total_liquido decimal(38,18), 
                vl_total_pedido decimal(38,18), 
                vl_troco decimal(38,18), 
                dt_process_stage timestamp(3)
            ) WITH (
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = 'A_RAIABD-TB_TELEVENDA',
              'connector.properties.bootstrap.servers' = '10.1.165.35:9092',
              'connector.properties.group.id' = 'test_3',
              'connector.properties.client.id' = '1',
              'connector.startup-mode' = 'latest-offset',
              'format.type' = 'json'
            )
            """

    ouput ="""
    CREATE TABLE dest(
                aa_validade_cartao_credito bigint, 
                cd_banco_cheque bigint, 
                cd_bandeira_cartao bigint, 
                cd_canal_internet string, 
                cd_carrier bigint, 
                cd_cmc7_cheque string, 
                cd_entregador bigint, 
                cd_filial bigint, 
                cd_filial_origem_pedido bigint, 
                cd_forma_pagto bigint, 
                cd_janela_apoio bigint, 
                cd_janela_pdv bigint, 
                cd_motivo_abandono_televenda bigint, 
                cd_motivo_televenda bigint, 
                cd_operador bigint, 
                cd_operador_baixa bigint, 
                cd_operador_fatura string, 
                cd_operador_modalidade string, 
                cd_operador_sangria string, 
                cd_origem_televenda bigint, 
                cd_status_televenda bigint, 
                cd_televenda bigint, 
                cd_televenda_pai bigint, 
                cd_televenda_turno string, 
                cd_tipo_cartao bigint, 
                cd_tipo_trans_cartao bigint, 
                ds_nsu string, 
                ds_nsu_cancelamento string, 
                ds_obs string, 
                ds_obs_dif_baixa string, 
                dthr_pedido timestamp(3), 
                dt_cheque timestamp(3), 
                dt_envio_superpolo timestamp(3), 
                dt_fatura timestamp(3), 
                dt_pedido timestamp(3), 
                dt_sangria timestamp(3), 
                dt_transacao_tef timestamp(3), 
                fl_acao_judicial bigint, 
                fl_agenda bigint, 
                fl_aguardando_baixa bigint, 
                fl_aguardando_ped_venda bigint, 
                fl_convenio bigint, 
                fl_devolucao bigint, 
                fl_emergencial bigint, 
                fl_endereco_novo bigint, 
                fl_escaneado bigint, 
                fl_frete_liberado bigint, 
                fl_imprimir_offline string, 
                fl_manutencao bigint, 
                fl_orcamento bigint, 
                hr_atend_previsto timestamp(3), 
                hr_entrega timestamp(3), 
                hr_fatura timestamp(3), 
                hr_impressao timestamp(3), 
                hr_retorno_entrega timestamp(3), 
                hr_saida_entrega timestamp(3), 
                id_cliente bigint, 
                ip_operador string, 
                md_duracao_atendimento_s bigint, 
                mm_validade_cartao_credito bigint, 
                nm_recebeu string, 
                nr_agencia bigint, 
                nr_autorizacao_pbm bigint, 
                nr_cartao_credito string, 
                nr_cep string, 
                nr_cheque bigint, 
                nr_codigo_seguranca string, 
                nr_conta_corrente string, 
                nr_cpf_cheque string, 
                nr_cpf_cnpj_nota string, 
                nr_cupom bigint, 
                nr_dependente_idade bigint, 
                nr_endereco bigint, 
                nr_objeto string, 
                nr_pedido bigint, 
                nr_pedido_devolucao string, 
                nr_relatorio_sangria string, 
                nr_rg_recebeu string, 
                nr_romaneio_entrega bigint, 
                nr_rot_dist bigint, 
                nr_sequencia_cupom bigint, 
                nr_seq_pedido bigint, 
                pc_desc decimal(38,18), 
                qt_parcelas bigint, 
                qt_pontos bigint, 
                qt_roteirizacao bigint, 
                qt_total bigint, 
                qt_total_medicamento bigint, 
                qt_total_perfumaria bigint, 
                st_retira_caixa string, 
                vl_abatimento decimal(38,18), 
                vl_abatimento_vista decimal(38,18), 
                vl_arredondamento decimal(38,18), 
                vl_custo_frete decimal(38,18), 
                vl_desc_cartao decimal(38,18), 
                vl_desc_faixa1 decimal(38,18), 
                vl_desc_oferta decimal(38,18), 
                vl_diferenca_baixa decimal(38,18), 
                vl_diferenca_devol_cliente decimal(38,18), 
                vl_dinheiro decimal(38,18), 
                vl_frete decimal(38,18), 
                vl_gorjeta decimal(38,18), 
                vl_nf_devolucao decimal(38,18), 
                vl_repasse decimal(38,18), 
                vl_subsidio_empresa decimal(38,18), 
                vl_total decimal(38,18), 
                vl_total_liquido decimal(38,18), 
                vl_total_pedido decimal(38,18), 
                vl_troco decimal(38,18), 
                dt_process_stage timestamp(3)
                ) with(
                    'connector.type' = 'filesystem',
                    'path'='s3://rd-datalake-dev-temp/spark_dev/flink/out.csv',
                    'format'='parquet')
                """
   # Seta enviroments
    s_env = StreamExecutionEnvironment.get_execution_environment()
    st_env = StreamTableEnvironment.create(stream_execution_environment=s_env)
    # Executa statements
    st_env.execute_sql(input)
    st_env.execute_sql(ouput)

    st_env.from_path("source")\
    .select("*")\
    .execute_insert("dest")

if __name__ == '__main__':
    main()
