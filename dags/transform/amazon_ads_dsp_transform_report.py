from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import json

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

def clean_and_transform_data(**kwargs):
    raw_data = Variable.get("dsp_report_data")
    data = json.loads(raw_data)
    df = pd.DataFrame(data)

    df["date"] = pd.to_datetime(df["date"], unit="ms").dt.date
    df["orderStartDate"] = pd.to_datetime(df["orderStartDate"], unit="ms").dt.date
    df["orderEndDate"] = pd.to_datetime(df["orderEndDate"], unit="ms").dt.date
    df["lineItemStartDate"] = pd.to_datetime(df["lineItemStartDate"], unit="ms").dt.date
    df["lineItemEndDate"] = pd.to_datetime(df["lineItemEndDate"], unit="ms").dt.date

    selected_columns = {
        "date": "date",
        "advertiserId": "advertiserId",
        "advertiserName": "advertiserName",
        "orderId": "campaignId",
        "orderName": "campaign",
        "orderBudget": "campaignBudget",
        "orderStartDate": "campaignStartDate",
        "orderEndDate": "campaignEndDate",
        "lineItemId": "lineItemId",
        "lineItemName": "lineItemName",
        "lineItemStartDate": "lineItemStartDate",
        "lineItemEndDate": "lineItemEndDate",
        "lineItemBudget": "lineItemBudget",
        "creativeID": "creativeId",
        "creativeName": "creativeNameCreative Name",
        "creativeType": "creativeType",
        "totalCost": "totalCost",
        "impressions": "impressions",
        "clickThroughs": "clicks",
        "totalPurchases14d": "totalPurchases14d",
        "totalUnitsSold14d": "totalUnitsSold14d",
        "totalNewToBrandUnitsSold14d": "totalNewToBrandUnitsSold14d",
        "sales14d": "sales14d",
        "totalSubscribeAndSaveSubscriptions14d": "totalSubscribeAndSaveSubscriptions14d",
        "totalDetailPageViews14d": "totalDetailPageViews14d",
        "totalAddToCart14d": "totalAddToCart14d"
    }
    df = df.rename(columns=selected_columns)[selected_columns.values()]
    df_json = df.to_json(orient='records') 
    Variable.set("dsp_report_df", df_json)

with DAG(
    "clean_transform_dsp_data",
    default_args=default_args,
    description="DAG para limpeza e transformação de dados do Amazon DSP",
    schedule_interval=None, 
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    clean_data_task = PythonOperator(
        task_id="clean_transform_data",
        python_callable=clean_and_transform_data,
        provide_context=True,
    )

    clean_data_task