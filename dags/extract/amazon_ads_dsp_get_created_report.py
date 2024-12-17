from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import time
import requests
import json
import os

API_URL = os.getenv("AMAZON_ADS_API_URL")
AMAZON_ADS_DSP_ACCOUNT_ID = os.getenv("AMAZON_ADS_DSP_ACCOUNT_ID")
DSP_REPORT_ENDPOINT = f"{API_URL}/accounts/{AMAZON_ADS_DSP_ACCOUNT_ID}/dsp/reports"

ACCESS_TOKEN = Variable.get("amazon_access_token")

def download_report(**kwargs):
    report_metadata = Variable.get("dsp_report_metadata", default_var=None)
    report_location = report_metadata['location']
    if not report_location:
        raise Exception("URL do relatório não encontrada nos XComs.")
    response = requests.get(report_location)
    if response.status_code == 200:
        report_data = response.json()
        Variable.set("dsp_report_data", json.dumps(report_data))
        print("Dados do relatório baixados com sucesso.")
    else:
        raise Exception(f"Erro ao baixar relatório: {response.status_code} - {response.text}")

def check_report_status(**kwargs):
    report_id = Variable.get("amazon_dsp_report_id", default_var=None)
    if not report_id: 
        raise Exception("Report ID não encontrado nas variáveis.")

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Amazon-Advertising-API-ClientId": os.getenv("AMAZON_ADS_CLIENT_ID"),
        "Accept": "application/vnd.dspgetreports.v3+json"
    }

    status_endpoint = f"{DSP_REPORT_ENDPOINT}/{report_id}"

    max_retries = 20  # Número máximo de tentativas
    wait_time = 180   # Tempo de espera entre as tentativas (em segundos)

    for attempt in range(max_retries):
        response = requests.get(status_endpoint, headers=headers)
        print(response)
        if response.status_code == 200:
            report_data = response.json()
            status = report_data.get("status")
            if status == "SUCCESS":
                Variable.set("dsp_report_metadata", json.dumps(report_data))
                print("Relatório concluído com sucesso.")
                return
            elif status == "IN_PROGRESS":
                print(f"Tentativa {attempt + 1}/{max_retries}: Relatório ainda em progresso.")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
            else:
                raise Exception(f"Erro desconhecido no status do relatório: {status}")
        else:
            raise Exception(f"Erro ao verificar status do relatório: {response.status_code} - {response.text}")

    raise Exception("Relatório não foi concluído dentro do número máximo de tentativas.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "amazon_ads_dsp_get_created_report",
    default_args=default_args,
    description="DAG para obtenção do report DSP criado no Amazon Ads",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    get_dsp_report_task = PythonOperator(
        task_id="get_dsp_report",
        python_callable=check_report_status,
        provide_context=True,
    )

    download_dsp_report_task = PythonOperator(
        task_id="download_report",
        python_callable=download_report,
        provide_context=True,
    )

    get_dsp_report_task >> download_dsp_report_task