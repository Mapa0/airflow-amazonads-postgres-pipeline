from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import os

API_URL = os.getenv("AMAZON_ADS_API_URL")
AMAZON_ADS_DSP_ACCOUNT_ID = os.getenv("AMAZON_ADS_DSP_ACCOUNT_ID")
DSP_REPORT_ENDPOINT = f"{API_URL}/accounts/{AMAZON_ADS_DSP_ACCOUNT_ID}/dsp/reports"

ACCESS_TOKEN = Variable.get("amazon_access_token")

def create_dsp_report(**kwargs):
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = end_date 

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Amazon-Advertising-API-ClientId": os.getenv("AMAZON_ADS_CLIENT_ID"),
        "Accept": "application/vnd.dspcreatereports.v3+json"
    }

    payload = {
        "startDate": start_date,
        "endDate": end_date,
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
        report_data = response.json()
        report_id = report_data.get("reportId")
        Variable.set("amazon_dsp_report_id", report_id)
        print(f"Relatório em processamento. Report ID: {report_id}")
    elif response.status_code == 200:
        report_data = response.json()
        Variable.set("amazon_dsp_report_id", report_id)
        print("Relatório extraído com sucesso.")
    else:
        raise Exception(f"Erro ao extrair dados: {response.status_code} - {response.text}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
}

with DAG(
    "amazon_ads_dsp_create_report",
    default_args=default_args,
    description="DAG para extração de dados do Amazon DSP",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    create_dsp_report_task = PythonOperator(
        task_id="create_dsp_report",
        python_callable=create_dsp_report,
        provide_context=True,
    )

    create_dsp_report_task
