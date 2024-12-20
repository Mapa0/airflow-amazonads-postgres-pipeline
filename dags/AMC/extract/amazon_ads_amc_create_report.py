from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import time
import requests
import uuid
import io
import os
import psycopg2
from psycopg2.extras import execute_values
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv
from airflow.exceptions import AirflowException

load_dotenv()

API_URL = os.getenv("AMAZON_ADS_AMC_API_URL")
CLIENT_ID = os.getenv("AMAZON_ADS_CLIENT_ID")
ADVERTISER_ID = os.getenv("AMAZON_ADS_API_ADVERTISER_ID")
MARKETPLACE_ID = os.getenv("AMAZON_ADS_API_MARKETPLACE_ID")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def set_parameters(**kwargs):
    ti = kwargs['ti']
    
    days_offset = kwargs.get('days_offset', 1) 
    analysis_window = kwargs.get('analysis_window', 7) 

    end_date = datetime.now().date() - timedelta(days=days_offset)
    start_date = end_date - timedelta(days=analysis_window)

    time_window_start = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    time_window_end = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    query = kwargs.get('query', 'SELECT 0')
    table_name = kwargs.get('table_name', 'default_table')

    ti.xcom_push(key='sql_query', value=query)
    ti.xcom_push(key='table_name', value=table_name)
    ti.xcom_push(key='timeWindowStart', value=time_window_start)
    ti.xcom_push(key='timeWindowEnd', value=time_window_end)

    print(f"Time Window Start: {time_window_start}")
    print(f"Time Window End: {time_window_end}")
    print(f"Query: {query}")
    print(f"Table Name: {table_name}")

def create_amc_workflow(**kwargs):
    CREATE_WORKFLOW_URL = f"{API_URL}/workflows"
    ACCESS_TOKEN = Variable.get("amazon_access_token")
    workflow_id = str(uuid.uuid4())
    
    sql_query = kwargs['ti'].xcom_pull(task_ids='set_parameters', key='sql_query')

    headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
    "Amazon-Advertising-API-ClientID": CLIENT_ID,
    "Amazon-Advertising-API-AdvertiserId": ADVERTISER_ID,
    "Amazon-Advertising-API-MarketplaceId": MARKETPLACE_ID,
    "Accept": "application/vnd.amcworkflows.v1+json" 
    }

    payload = {
        "distinctUserCountColumn": "du_count",
        "filteredMetricsDiscriminatorColumn": "filtered",
        "filteredReasonColumn": "true",
        "sqlQuery": sql_query,
        "workflowId": workflow_id,
    }

    response = requests.post(CREATE_WORKFLOW_URL, headers=headers, json=payload)

    if response.status_code in [200, 202]:
        print(f"Sucesso na criação do workflow. ID: {workflow_id}")
        kwargs['ti'].xcom_push(key='amc_workflow_id', value=workflow_id)
    else:
        raise Exception(f"Erro na requisição: {response.status_code} - {response.text}")

def create_workflow_execution(**kwargs):
    ACCESS_TOKEN = Variable.get("amazon_access_token")
    execution_endpoint = f"{API_URL}/workflowExecutions"

    workflow_id = kwargs['ti'].xcom_pull(task_ids='create_amc_workflow', key='amc_workflow_id')
    time_window_start = kwargs['ti'].xcom_pull(task_ids='set_parameters', key='timeWindowStart')
    time_window_end = kwargs['ti'].xcom_pull(task_ids='set_parameters', key='timeWindowEnd')

    if not workflow_id:
        raise Exception("Workflow ID não encontrado. Certifique-se de que o workflow foi criado com sucesso.")

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Amazon-Advertising-API-ClientID": CLIENT_ID,
        "Amazon-Advertising-API-AdvertiserId": ADVERTISER_ID,
        "Amazon-Advertising-API-MarketplaceId": MARKETPLACE_ID,
        "Accept": "application/vnd.amcworkflowexecutions.v1+json"
    }
    
    payload = {
        "timeWindowStart": time_window_start,
        "timeWindowEnd": time_window_end,
        "timeWindowType": "EXPLICIT",
        "dryRun": False,
        "workflowId": workflow_id,
        "skipPublish": False,
        "disableAggregationControls": False
    }

    response = requests.post(execution_endpoint, headers=headers, json=payload)

    if response.status_code == 202 or response.status_code == 200:
        execution_data = response.json()
        workflowExecutionId = execution_data.get("workflowExecutionId")
        kwargs['ti'].xcom_push(key='workflowExecutionId', value=workflowExecutionId)
        print(f"Execução do workflow criada com sucesso. Execution ID: {workflowExecutionId}")
    else:
        raise Exception(f"Erro ao criar execução do workflow: {response.status_code} - {response.text}")

def monitor_workflow_execution(**kwargs):
    ACCESS_TOKEN = Variable.get("amazon_access_token")

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Amazon-Advertising-API-ClientID": os.getenv("AMAZON_ADS_CLIENT_ID"),
        "Amazon-Advertising-API-AdvertiserId": os.getenv("AMAZON_ADS_API_ADVERTISER_ID"),
        "Amazon-Advertising-API-MarketplaceId": os.getenv("AMAZON_ADS_API_MARKETPLACE_ID"),
        "Accept": "application/vnd.amcworkflowexecutions.v1+json"
    }

    # Recupera o workflowExecutionId das XComs
    workflow_execution_id = kwargs['ti'].xcom_pull(task_ids='create_amc_workflow_execution', key='workflowExecutionId')

    if not workflow_execution_id:
        raise Exception("Workflow Execution ID não encontrado. Certifique-se de que a execução foi criada com sucesso.")

    execution_endpoint = f"{API_URL}/workflowExecutions/{workflow_execution_id}"

    while True:
        response = requests.get(execution_endpoint, headers=headers)
        print(response.json())
        if response.status_code == 200:
            execution_data = response.json()
            status = execution_data.get("status")

            print(f"Status atual do workflow execution {workflow_execution_id}: {status}")

            if status == "SUCCEEDED":
                print(f"Workflow Execution SUCCEEDED. ID: {workflow_execution_id}")
                break

            elif status in ["FAILED", "CANCELLED"]:
                raise Exception(f"Workflow Execution {workflow_execution_id} terminou com status: {status}")

            else:
                print("Workflow ainda em progresso. Aguardando 60 segundos antes de tentar novamente...")
                time.sleep(60)

        else:
            raise Exception(f"Erro ao verificar status do workflow execution: {response.status_code} - {response.text}")

def get_download_url(**kwargs):
    ACCESS_TOKEN = Variable.get("amazon_access_token")
    
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Amazon-Advertising-API-ClientID": os.getenv("AMAZON_ADS_CLIENT_ID"),
        "Amazon-Advertising-API-AdvertiserId": os.getenv("AMAZON_ADS_API_ADVERTISER_ID"),
        "Amazon-Advertising-API-MarketplaceId": os.getenv("AMAZON_ADS_API_MARKETPLACE_ID"),
        "Accept": "application/vnd.amcworkflows.v1+json"
    }

    workflow_execution_id = kwargs['ti'].xcom_pull(task_ids='create_amc_workflow_execution', key='workflowExecutionId')
    if not workflow_execution_id:
        raise AirflowException("Workflow Execution ID não encontrado. Certifique-se de que a execução foi bem-sucedida.")

    download_url_endpoint = f"{API_URL}/workflowExecutions/{workflow_execution_id}/downloadUrls"

    response = requests.get(download_url_endpoint, headers=headers)
    if response.status_code == 200:
        download_data = response.json()
        download_urls = download_data.get("downloadUrls", [])
        metadata_download_urls = download_data.get("metadataDownloadUrls", [])

        if not download_urls:
            raise AirflowException("Nenhuma URL de download encontrada no retorno da API.")

        kwargs['ti'].xcom_push(key='download_urls', value=download_urls)
        kwargs['ti'].xcom_push(key='metadata_download_urls', value=metadata_download_urls)
        print(f"URLs de download salvas: {download_urls}")
    else:
        raise AirflowException(f"Erro ao obter URLs de download: {response.status_code} - {response.text}")

def fetch_csv_to_dataframe(**kwargs):
    download_urls = kwargs['ti'].xcom_pull(task_ids='get_download_url', key='download_urls')

    if not download_urls or len(download_urls) == 0:
        raise Exception("Nenhuma URL de download encontrada nas XComs.")

    download_url = download_urls[0]
    print(f"Baixando dados de: {download_url}")

    response = requests.get(download_url)
    if response.status_code != 200:
        raise Exception(f"Erro ao obter o CSV: {response.status_code} - {response.text}")

    try:
        df = pd.read_csv(io.StringIO(response.text))
    except Exception as e:
        raise Exception(f"Erro ao carregar o CSV em um DataFrame: {e}")

    print("CSV carregado com sucesso no DataFrame.")

    # Remover linhas onde 'filtered' é True (se a coluna existir)
    if 'filtered' in df.columns:
        df = df[~df['filtered']]  # Mantém apenas linhas onde filtered é False

    # Dropar colunas 'du_count', 'true' e 'filtered' se existirem
    cols_to_drop = [col for col in ['du_count', 'true', 'filtered'] if col in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    time_window_start = kwargs['ti'].xcom_pull(task_ids='set_parameters', key='timeWindowStart')
    time_window_end = kwargs['ti'].xcom_pull(task_ids='set_parameters', key='timeWindowEnd')

    if not time_window_start or not time_window_end:
        raise Exception("Time Window Start ou End não encontrado nas XComs.")

    time_window_start_date = pd.to_datetime(time_window_start).strftime('%Y-%m-%d')
    time_window_end_date = pd.to_datetime(time_window_end).strftime('%Y-%m-%d')
    execution_date_date = pd.to_datetime(execution_date).strftime('%Y-%m-%d')

    df['execution_date'] = execution_date_date
    df['time_window_start'] = time_window_start_date
    df['time_window_end'] = time_window_end_date

    df_stringified = df.astype(str).to_dict()

    kwargs['ti'].xcom_push(key='csv_dataframe', value=df_stringified)

    print("Novas colunas adicionadas e DataFrame salvo nas XComs como strings.")

    return df

def insert_data_incrementally_auto(**kwargs):
    import psycopg2
    from psycopg2.extras import execute_values
    import pandas as pd
    import os

    # Configurações do banco de dados
    db_config = {
        'host': os.getenv('DB_ENDPOINT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT'),
    }

    # Obter DataFrame e nome da tabela das XComs
    df_dict = kwargs['ti'].xcom_pull(task_ids='fetch_csv_to_dataframe', key='csv_dataframe')
    table_name = kwargs['ti'].xcom_pull(task_ids='set_parameters', key='table_name')

    if not df_dict or not table_name:
        raise Exception("DataFrame ou nome da tabela não encontrado no XCom.")

    # Converter de dict para DataFrame
    df = pd.DataFrame.from_dict(df_dict)

    # Adicionar aspas duplas aos nomes das colunas para evitar erros de sintaxe
    column_names = [f'"{col}"' for col in df.columns]
    primary_keys = [f'"{col}"' for col in df.columns]  # Use todas as colunas como PK para simplicidade

    # Conectar ao banco de dados
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Criar tabela caso não exista
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f'{col} TEXT' for col in column_names])},
            PRIMARY KEY ({', '.join(primary_keys)})
        );
    """
    print(f"Query de criação de tabela:\n{create_table_query}")
    cursor.execute(create_table_query)

    # Inserir ou atualizar registros
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(column_names)})
        VALUES %s
        ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE SET
        {', '.join([f'{col} = EXCLUDED.{col}' for col in column_names])};
    """
    print(f"Query de inserção/atualização:\n{insert_query}")

    # Converter DataFrame para lista de registros
    records = df.astype(str).to_numpy().tolist()
    execute_values(cursor, insert_query, records)

    # Confirmar transação e fechar conexão
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Dados inseridos ou atualizados na tabela '{table_name}' com sucesso.")


query_cap = """
    --CAP de Campanhas--
WITH campaign_ids (campaign_id) AS (
    VALUES
    (579639688386477304),
    (578317514331189527),
    (590513643956296999),
    (591142927103694570)
),
user_conversions AS (
  SELECT
    user_id,
    campaign_id,
    campaign,
    conversion_event_dt AS conversion_time,
    purchases,
    units_sold,
    product_sales,
    add_to_cart,
    detail_page_view
  FROM
    amazon_attributed_events_by_traffic_time
  WHERE
    campaign_id IN (SELECT campaign_id FROM campaign_ids)
),
impressions_with_conversions AS (
  SELECT
    i.user_id,
    i.campaign_id,
    i.campaign,
    i.impression_dt AS impression_time,
    i.impressions,
    c.conversion_time,
    c.purchases,
    c.units_sold,
    c.product_sales,
    c.add_to_cart,
    c.detail_page_view
  FROM
    dsp_impressions i
    LEFT JOIN user_conversions c
      ON i.user_id = c.user_id
      AND i.campaign_id = c.campaign_id
      AND i.impression_dt <= c.conversion_time
  WHERE
    i.campaign_id IN (SELECT campaign_id FROM campaign_ids)
),
impressions_before_each_conversion AS (
  SELECT
    user_id,
    campaign_id,
    campaign,
    conversion_time,
    SUM(impressions) AS impressions_before_conversion,
    MAX(purchases) AS purchases,
    MAX(units_sold) AS units_sold,
    MAX(product_sales) AS product_sales,
    MAX(add_to_cart) AS add_to_cart,
    MAX(detail_page_view) AS detail_page_view
  FROM
    impressions_with_conversions
  WHERE
    conversion_time IS NOT NULL
  GROUP BY
    user_id,
    campaign_id,
    campaign,
    conversion_time
),
users_no_conversions AS (
  SELECT
    i.user_id,
    i.campaign_id,
    i.campaign,
    SUM(impressions) AS total_impressions
  FROM
    dsp_impressions i
  LEFT JOIN user_conversions c
    ON i.user_id = c.user_id
    AND i.campaign_id = c.campaign_id
  WHERE
    i.campaign_id IN (SELECT campaign_id FROM campaign_ids)
    AND c.user_id IS NULL
  GROUP BY
    i.user_id,
    i.campaign_id,
    i.campaign
),
events_by_user AS (
  SELECT
    user_id,
    campaign_id,
    campaign,
    impressions_before_conversion as impressions,
    LEAST(impressions_before_conversion, 25) AS frequency,
    purchases,
    units_sold,
    product_sales,
    add_to_cart,
    detail_page_view
  FROM
    impressions_before_each_conversion
  UNION ALL
  SELECT
    user_id,
    campaign_id,
    campaign,
    total_impressions as impressions,
    LEAST(total_impressions, 25) AS frequency,
    0 AS purchases,
    0 AS units_sold,
    0 AS product_sales,
    0 AS add_to_cart,
    0 AS detail_page_view
  FROM
    users_no_conversions
)
SELECT
  campaign_id,
  campaign,
  frequency AS frequency_bucket,
  SUM(purchases) AS purchases,
  SUM(units_sold) AS units_sold,
  SUM(product_sales) AS product_sales,
  SUM(add_to_cart) AS add_to_cart,
  SUM(detail_page_view) AS detail_page_view,
  COUNT(distinct user_id) AS users_in_bucket,
  SUM(impressions) AS impressions_in_bucket
FROM
  events_by_user
GROUP BY
  campaign_id,
  campaign,
  frequency
"""

with DAG(
    "analise_cap",
    default_args=default_args,
    description="DAG responsável pela análise CAP com a tempestividade x",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    set_params_task = PythonOperator(
        task_id="set_parameters",
        python_callable=set_parameters,
        op_kwargs={
            "days_offset": 30,  # End date será ontem
            "analysis_window": 30,  # Start date será 7 dias antes do end date
            "query": "SELECT DISTINCT campaign_id, campaign from dsp_impressions",
            "table_name": "amc_campaign_names",
        },
    )

    authenticate_task = TriggerDagRunOperator(
        task_id="trigger_authentication_dag",
        trigger_dag_id="amazon_ads_authentication",
        wait_for_completion=True, 
        conf={"message": "Iniciando autenticação"},
    )

    amazon_amc_create_workflow_task = PythonOperator(
        task_id="create_amc_workflow",
        python_callable=create_amc_workflow,
        provide_context=True
    )

    amazon_amc_create_workflow_execution_task = PythonOperator(
        task_id="create_amc_workflow_execution",
        python_callable=create_workflow_execution,
        provide_context=True
    )

    amazon_amc_monitor_workflow_execution_task = PythonOperator(
        task_id="monitor_amc_workflow_execution",
        python_callable=monitor_workflow_execution,
        provide_context=True
    )

    amazon_amc_get_download_url_task = PythonOperator(
        task_id="get_download_url",
        python_callable=get_download_url,
        provide_context=True
    )

    amazon_amc_fetch_csv_to_dataframe_task = PythonOperator(
        task_id="fetch_csv_to_dataframe",
        python_callable=fetch_csv_to_dataframe,
        provide_context=True
    )

    amazon_amc_insert_data_incrementally_task = PythonOperator(
        task_id="insert_data_incrementally",
        python_callable=insert_data_incrementally_auto,
        provide_context=True
    )

    set_params_task >> authenticate_task >> amazon_amc_create_workflow_task >> amazon_amc_create_workflow_execution_task >> amazon_amc_monitor_workflow_execution_task >> amazon_amc_get_download_url_task >> amazon_amc_fetch_csv_to_dataframe_task >> amazon_amc_insert_data_incrementally_task
