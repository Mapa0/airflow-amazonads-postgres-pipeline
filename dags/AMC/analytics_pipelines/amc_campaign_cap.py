from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv
from dags.AMC.extract.amazon_ads_amc_extract import AmazonAdsAmcExtract
from dags.AMC.transform.amazon_ads_amc_transform import AmazonAdsAmcTransform
from dags.AMC.load.amazon_ads_amc_load import AmazonAdsAmcLoad
from include.parameters import Parameters
from include.queries import Query
from airflow.utils.task_group import TaskGroup


load_dotenv()
amc_extract = AmazonAdsAmcExtract()
amc_transform = AmazonAdsAmcTransform()
amc_load = AmazonAdsAmcLoad()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "amc_campaign_cap",
    default_args=default_args,
    description="DAG responsável pela análise CAP com a tempestividade de 30 dias",
    schedule_interval='0 0 1 * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # TaskGroup: Setup
    with TaskGroup("setup", tooltip="Configuração de parâmetros e autenticação") as setup:
        set_params_task = PythonOperator(
            task_id="set_parameters",
            python_callable=Parameters.set_parameters,
            op_kwargs={
                "days_offset": 14,  # End date será ontem
                "analysis_window": 30,  # Start date será 7 dias antes do end date
                "query": Query.cap,
                "table_name": "amc_campaign_cap",
            },
        )

        authenticate_task = TriggerDagRunOperator(
            task_id="trigger_authentication_dag",
            trigger_dag_id="amazon_ads_authentication",
            wait_for_completion=True, 
            conf={"message": "Iniciando autenticação"},
        )

        set_params_task >> authenticate_task

    with TaskGroup("extract", tooltip="Extração de dados da campanha") as extract:
        create_workflow_task = PythonOperator(
            task_id="create_amc_workflow",
            python_callable=amc_extract.create_amc_workflow,
        )

        create_workflow_execution_task = PythonOperator(
            task_id="create_amc_workflow_execution",
            python_callable=amc_extract.create_workflow_execution,
        )

        monitor_workflow_execution_task = PythonOperator(
            task_id="monitor_amc_workflow_execution",
            python_callable=amc_extract.monitor_workflow_execution,
        )

        get_download_url_task = PythonOperator(
            task_id="get_download_url",
            python_callable=amc_extract.get_download_url,
        )

        extract_csv_content_task = PythonOperator(
            task_id="extract_csv_content",
            python_callable=amc_extract.extract_csv_content,
        )

        create_workflow_task >> create_workflow_execution_task >> monitor_workflow_execution_task >> get_download_url_task >> extract_csv_content_task

    with TaskGroup("transform", tooltip="Transformação dos dados extraídos") as transform:
        transform_csv_to_dataframe_task = PythonOperator(
            task_id="transform_csv_to_dataframe",
            python_callable=amc_transform.transform_csv_to_dataframe,
        )

    with TaskGroup("load", tooltip="Carregamento dos dados transformados") as load:
        insert_data_incrementally_task = PythonOperator(
            task_id="insert_data_incrementally",
            python_callable=amc_load.insert_data_incrementally_auto,
        )

    setup >> extract >> transform >> load