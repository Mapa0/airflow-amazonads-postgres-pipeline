from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "amazon_ads_dsp_main",
    default_args=default_args,
    description="Fluxo principal para autenticação, criação e extração de relatórios na Amazon Ads",
    schedule_interval="0 6 * * *",  
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    authenticate_task = TriggerDagRunOperator(
        task_id="trigger_authentication_dag",
        trigger_dag_id="amazon_ads_authentication",
        wait_for_completion=True, 
        conf={"message": "Iniciando autenticação"},
    )
    create_report_task = TriggerDagRunOperator(
        task_id="trigger_create_dsp_report",
        trigger_dag_id="amazon_ads_dsp_create_report",
        conf={"message": "Autenticação concluída. Iniciando criação de relatório."},
        wait_for_completion=True,
    )
    extract_dsp_report_task = TriggerDagRunOperator(
        task_id="trigger_get_dsp_report",
        trigger_dag_id="amazon_ads_dsp_get_created_report",
        conf={"message": "Solicitação de criação do relatório enviada. Iniciando processo de obtenção do relatório."},
        wait_for_completion=True,
    )
    transform_report_task = TriggerDagRunOperator(
        task_id="trigger_transform_dsp_report",
        trigger_dag_id="clean_transform_dsp_data", 
        conf={"message": "Relatório obtido. Iniciando transformação do relatório."},
        wait_for_completion=True,
    )
    load_report_task = TriggerDagRunOperator(
        task_id="trigger_load_dsp_report",
        trigger_dag_id="amazon_ads_dsp_to_rds",
        conf={"message": "Transformação completa, iniciando carga no banco de dados."},
        wait_for_completion=True,
    )
    authenticate_task >> create_report_task >> extract_dsp_report_task >> transform_report_task >> load_report_task
