from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import json

# Configurações gerais
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Função de limpeza e transformação
def clean_and_transform_data(**kwargs):
    # Carregar os dados do XCom ou de um arquivo
    raw_data = Variable.get("dsp_report_data")

    data = json.loads(raw_data)

    print(data)

    # Criar o DataFrame
    df = pd.DataFrame(data)

    # Converter timestamps para datas legíveis
    df["date"] = pd.to_datetime(df["date"], unit='ms')
    df["orderStartDate"] = pd.to_datetime(df["orderStartDate"], unit='ms')
    df["orderEndDate"] = pd.to_datetime(df["orderEndDate"], unit='ms')

    # Selecionar e renomear colunas de interesse
    selected_columns = {
        "date": "Report Date",
        "orderName": "Order Name",
        "lineItemName": "Line Item Name",
        "advertiserName": "Advertiser Name",
        "impressions": "Impressions",
        "clickThroughs": "Clicks",
        "sales14d": "Sales (14 Days)",
        "totalDetailPageViews14d": "Detail Page Views (14 Days)",
    }
    df = df.rename(columns=selected_columns)[selected_columns.values()]

    print(df.head(5))

# Configuração da DAG
with DAG(
    "clean_transform_dsp_data",
    default_args=default_args,
    description="DAG para limpeza e transformação de dados do Amazon DSP",
    schedule_interval=None,  # Acionada manualmente ou por outra DAG
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    clean_data_task = PythonOperator(
        task_id="clean_transform_data",
        python_callable=clean_and_transform_data,
        provide_context=True,
    )

    clean_data_task