from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import os

# Configuração da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "amazon_ads_main",
    default_args=default_args,
    description="Fluxo principal para autenticação, criação e extração de relatórios na Amazon Ads",
    schedule_interval=None,  # Executa apenas quando acionado
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    """
    # Tarefa 1: Acionar a DAG de autenticação do Amazon Ads
    authenticate_task = TriggerDagRunOperator(
        task_id="trigger_authentication_dag",
        trigger_dag_id="amazon_ads_authentication",  # Nome da DAG de autenticação
        wait_for_completion=True,  # Aguarda a conclusão da DAG de autenticação
        conf={"message": "Iniciando autenticação"},  # Mensagem opcional para a DAG acionada
    )

    # Tarefa 2: Criar relatório via Amazon Ads API
    create_report_task = TriggerDagRunOperator(
        task_id="trigger_create_dsp_report",
        trigger_dag_id="amazon_ads_dsp_create_report",  # Nome da DAG que cria o relatório
        conf={"message": "Autenticação concluída. Iniciando criação de relatório."},  # Mensagem opcional
        wait_for_completion=True,  # Espera a DAG alvo terminar para continuar
    )
    """
    # Tarefa 3: Obter relatório via Amazon Ads API
    get_dsp_report_task = TriggerDagRunOperator(
        task_id="trigger_get_dsp_report",
        trigger_dag_id="amazon_ads_dsp_get_created_report",  # Nome da DAG que obtém o relatório
        conf={"message": "Solicitação de criação do relatório enviada. Iniciando processo de obtenção do relatório."},  # Mensagem opcional
        wait_for_completion=True,  # Espera a DAG alvo terminar para continuar
    )
    """
    # Tarefa 4: Obter relatório via Amazon Ads API
    transform_report_task = TriggerDagRunOperator(
        task_id="trigger_transform_dsp_report",
        trigger_dag_id="clean_transform_dsp_data",  # Nome da DAG que obtém o relatório
        conf={"message": "Solicitação de criação do relatório enviada. Iniciando processo de obtenção do relatório."},  # Mensagem opcional
        wait_for_completion=True,  # Espera a DAG alvo terminar para continuar
    )
    """
    # Configurar dependências entre as tarefas
    """authenticate_task >> create_report_task >> get_report_task"""
    get_dsp_report_task