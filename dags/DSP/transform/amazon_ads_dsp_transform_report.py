import pandas as pd
import json

class AmazonAdsDspReportTransform:
    def __init__(self):
        pass

    def clean_and_transform_data(self, **kwargs):
        raw_data = kwargs['ti'].xcom_pull(task_ids='extract.download_report', key='dsp_report_data')
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
        kwargs['ti'].xcom_push(key='dsp_report_df', value=df_json)