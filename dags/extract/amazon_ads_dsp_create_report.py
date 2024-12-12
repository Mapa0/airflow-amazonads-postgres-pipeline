from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import os

# Variáveis de ambiente e configuração
API_URL = os.getenv("AMAZON_ADS_API_URL")
AMAZON_ADS_DSP_ACCOUNT_ID = os.getenv("AMAZON_ADS_DSP_ACCOUNT_ID")
DSP_REPORT_ENDPOINT = f"{API_URL}/accounts/{AMAZON_ADS_DSP_ACCOUNT_ID}/dsp/reports"

# Carregar o token armazenado no Airflow
ACCESS_TOKEN = Variable.get("amazon_access_token")

# Função de extração de dados
import time

def create_dsp_report(**kwargs):
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Amazon-Advertising-API-ClientId": os.getenv("AMAZON_ADS_CLIENT_ID"),
        "Accept": "application/vnd.dspcreatereports.v3+json"
    }

    payload = {
        "startDate": "2024-11-21",
        "endDate": "2024-11-27",
        "type": "CAMPAIGN",
        "dimensions": [
            "ORDER",
            "LINE_ITEM",
            "CREATIVE"
        ],
        "metrics": [
            "impressions", 
            "clickThroughs", 
            "totalCost",
            "sales14d",
            "totalDetailPageViews14d",
            "totalDetailPageClicks14d",
            "totalNewToBrandUnitsSold14d",
            "totalPurchases14d",
            "totalUnitsSold14d",
            "totalAddToCart14d",
            "totalSubscribeAndSaveSubscriptions14d"
        ],
        "timeUnit": "DAILY",
        "format": "JSON"
    }

    response = requests.post(DSP_REPORT_ENDPOINT, headers=headers, json=payload)

    if response.status_code == 202:
        # Extração bem-sucedida, mas relatório está sendo processado
        report_data = response.json()
        report_id = report_data.get("reportId")
        kwargs['ti'].xcom_push(key='report_id', value=report_id)
        Variable.set("amazon_dsp_report_id", report_id)
        print(f"Relatório em processamento. Report ID: {report_id}")
    elif response.status_code == 200:
        # Caso excepcional, onde o relatório já está pronto
        report_data = response.json()
        kwargs['ti'].xcom_push(key='dsp_report_data', value=report_data)
        print("Relatório extraído com sucesso.")
    else:
        raise Exception(f"Erro ao extrair dados: {response.status_code} - {response.text}")

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
    "amazon_ads_dsp_create_report",
    default_args=default_args,
    description="DAG para extração de dados do Amazon DSP",
    schedule_interval=None,  # Esta DAG é acionada manualmente ou por outra DAG
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Tarefa 1: Extração de dados da API
    create_dsp_report_task = PythonOperator(
        task_id="create_dsp_report",
        python_callable=create_dsp_report,
        provide_context=True,
    )

    create_dsp_report_task