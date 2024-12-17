from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
import json
import numpy as np
from dotenv import load_dotenv
import os

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

load_dotenv()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

DB_CONFIG = {
    "host": os.getenv("DB_ENDPOINT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", 5432),
}

TABLE_NAME = "amazon_ads_dsp_report"

def create_table_if_not_exists():
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        date DATE,
        advertiserId BIGINT,
        advertiserName TEXT,
        campaignId BIGINT,
        campaign TEXT,
        campaignBudget FLOAT,
        campaignStartDate DATE,
        campaignEndDate DATE,
        lineItemId BIGINT,
        lineItemName TEXT,
        lineItemStartDate DATE,
        lineItemEndDate DATE,
        lineItemBudget FLOAT,
        creativeId BIGINT,
        creativeName TEXT,
        creativeType TEXT,
        totalCost FLOAT,
        impressions BIGINT,
        clicks BIGINT,
        totalPurchases14d BIGINT,
        totalUnitsSold14d BIGINT,
        totalNewToBrandUnitsSold14d BIGINT,
        sales14d FLOAT,
        totalSubscribeAndSaveSubscriptions14d BIGINT,
        totalDetailPageViews14d BIGINT,
        totalAddToCart14d BIGINT,
        PRIMARY KEY (date, campaignId, lineItemId, creativeId)
    );
    """

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def insert_incremental(**kwargs):
    register_adapter(np.float64, addapt_numpy_float64)
    register_adapter(np.int64, addapt_numpy_int64)
    raw_data = Variable.get("dsp_report_df")
    
    try:
        report_data = json.loads(raw_data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Erro ao decodificar JSON da variável: {e}")
    
    if isinstance(report_data, list) and all(isinstance(item, dict) for item in report_data):
        df = pd.DataFrame(report_data)
    else:
        raise ValueError("O JSON não está no formato esperado: uma lista de dicionários.")

    df["date"] = pd.to_datetime(df["date"], unit="ms").dt.date
    df["campaignStartDate"] = pd.to_datetime(df["campaignStartDate"], unit="ms").dt.date
    df["campaignEndDate"] = pd.to_datetime(df["campaignEndDate"], unit="ms").dt.date
    df["lineItemStartDate"] = pd.to_datetime(df["lineItemStartDate"], unit="ms").dt.date
    df["lineItemEndDate"] = pd.to_datetime(df["lineItemEndDate"], unit="ms").dt.date

    for column in df.select_dtypes(include=[np.number]).columns:
        df[column] = df[column].astype(float if df[column].dtype == np.float64 else int)
        print(column)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        date, advertiserId, advertiserName, campaignId, campaign, campaignBudget, campaignStartDate,
        campaignEndDate, lineItemId, lineItemName, lineItemStartDate, lineItemEndDate,
        lineItemBudget, creativeId, creativeName, creativeType, totalCost, impressions, clicks,
        totalPurchases14d, totalUnitsSold14d, totalNewToBrandUnitsSold14d, sales14d,
        totalSubscribeAndSaveSubscriptions14d, totalDetailPageViews14d, totalAddToCart14d
    ) VALUES %s
    ON CONFLICT (date, campaignId, lineItemId, creativeId) DO UPDATE SET
        advertiserName = EXCLUDED.advertiserName,
        campaign = EXCLUDED.campaign,
        campaignBudget = EXCLUDED.campaignBudget,
        campaignStartDate = EXCLUDED.campaignStartDate,
        campaignEndDate = EXCLUDED.campaignEndDate,
        lineItemName = EXCLUDED.lineItemName,
        lineItemStartDate = EXCLUDED.lineItemStartDate,
        lineItemEndDate = EXCLUDED.lineItemEndDate,
        lineItemBudget = EXCLUDED.lineItemBudget,
        creativeName = EXCLUDED.creativeName,
        creativeType = EXCLUDED.creativeType,
        totalCost = EXCLUDED.totalCost,
        impressions = EXCLUDED.impressions,
        clicks = EXCLUDED.clicks,
        totalPurchases14d = EXCLUDED.totalPurchases14d,
        totalUnitsSold14d = EXCLUDED.totalUnitsSold14d,
        totalNewToBrandUnitsSold14d = EXCLUDED.totalNewToBrandUnitsSold14d,
        sales14d = EXCLUDED.sales14d,
        totalSubscribeAndSaveSubscriptions14d = EXCLUDED.totalSubscribeAndSaveSubscriptions14d,
        totalDetailPageViews14d = EXCLUDED.totalDetailPageViews14d,
        totalAddToCart14d = EXCLUDED.totalAddToCart14d;
    """

    records = df.to_records(index=False)
    execute_values(cursor, insert_query, records)

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    "amazon_ads_dsp_to_rds",
    default_args=default_args,
    description="DAG para salvar dados do relatório DSP no PostgreSQL",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table_if_not_exists,
    )

    insert_data_task = PythonOperator(
        task_id="insert_incremental_data",
        python_callable=insert_incremental,
        provide_context=True,
    )

    create_table_task >> insert_data_task
