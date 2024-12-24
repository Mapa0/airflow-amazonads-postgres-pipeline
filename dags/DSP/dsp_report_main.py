from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from dags.DSP.extract.amazon_ads_dsp_report_extract import AmazonAdsDspReportExtract
from dags.DSP.transform.amazon_ads_dsp_transform_report import AmazonAdsDspReportTransform
from dags.DSP.load.amazon_ads_dsp_report_load import AmazonAdsDspReportLoad
from dotenv import load_dotenv

load_dotenv()

dsp_extract = AmazonAdsDspReportExtract()
dsp_transform = AmazonAdsDspReportTransform()
dsp_load = AmazonAdsDspReportLoad()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
}

with DAG(
    "amazon_ads_dsp_report",
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
    create_report_task = PythonOperator(
        task_id="create_dsp_report",
        python_callable=dsp_extract.create_dsp_report,
        provide_context=True,
    )
    check_report_task = PythonOperator(
        task_id="get_dsp_report",
        python_callable=dsp_extract.check_report_status,
        provide_context=True,
    )
    download_dsp_report_task = PythonOperator(
        task_id="download_report",
        python_callable=dsp_extract.download_report,
        provide_context=True,
    )
    clean_data_task = PythonOperator(
        task_id="clean_transform_data",
        python_callable=dsp_transform.clean_and_transform_data,
        provide_context=True,
    )
    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=dsp_load.create_table_if_not_exists,
    )
    insert_data_task = PythonOperator(
        task_id="insert_incremental_data",
        python_callable=dsp_load.insert_incremental,
        provide_context=True,
    )

    authenticate_task >> create_report_task >> check_report_task >> download_dsp_report_task >> clean_data_task >> create_table_task >> insert_data_task