from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import os
import json
import time

class AmazonAdsDspReportExtract:
    def __init__(self):
        self.API_URL = os.getenv("AMAZON_ADS_API_URL")
        self.ACCESS_TOKEN = Variable.get("amazon_access_token")
        self.AMAZON_ADS_DSP_ACCOUNT_ID = os.getenv("AMAZON_ADS_DSP_ACCOUNT_ID")
        self.AMAZON_ADS_CLIENT_ID = os.getenv("AMAZON_ADS_CLIENT_ID")
        self.DSP_REPORT_ENDPOINT = f"{self.API_URL}/accounts/{self.AMAZON_ADS_DSP_ACCOUNT_ID}/dsp/reports"
        
    def create_dsp_report(self, **kwargs):

        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = end_date 

        headers = {
            "Authorization": f"Bearer {self.ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientId": self.AMAZON_ADS_CLIENT_ID,
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

        response = requests.post(self.DSP_REPORT_ENDPOINT, headers=headers, json=payload)

        if response.status_code == 202:
            report_data = response.json()
            report_id = report_data.get("reportId")
            kwargs['ti'].xcom_push(key='amazon_dsp_report_id', value=report_id)
            print(f"Relatório em processamento. Report ID: {report_id}")
        elif response.status_code == 200:
            report_data = response.json()
            kwargs['ti'].xcom_push(key='amazon_dsp_report_id', value=report_id)
            print("Relatório extraído com sucesso.")
        else:
            raise Exception(f"Erro ao extrair dados: {response.status_code} - {response.text}")

    def check_report_status(self,**kwargs):
        report_id = kwargs['ti'].xcom_pull(task_ids='extract.create_dsp_report', key='amazon_dsp_report_id')
        if not report_id: 
            raise Exception("Report ID não encontrado nas XComs.")

        headers = {
            "Authorization": f"Bearer {self.ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientId": self.AMAZON_ADS_CLIENT_ID,
            "Accept": "application/vnd.dspgetreports.v3+json"
        }

        status_endpoint = f"{self.DSP_REPORT_ENDPOINT}/{report_id}"

        max_retries = 20  # Número máximo de tentativas
        wait_time = 180   # Tempo de espera entre as tentativas (em segundos)

        for attempt in range(max_retries):
            response = requests.get(status_endpoint, headers=headers)
            print(response)
            if response.status_code == 200:
                report_data = response.json()
                status = report_data.get("status")
                if status == "SUCCESS":
                    kwargs['ti'].xcom_push(key='dsp_report_metadata', value=json.dumps(report_data))
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
    
    def download_report(self, **kwargs):
        report_metadata = kwargs['ti'].xcom_pull(task_ids='extract.get_dsp_report', key='dsp_report_metadata')
        
        if not report_metadata:
            raise Exception("Report Metadata invalid")
        report_location = json.loads(report_metadata)['location']
        if not report_location:
            raise Exception("URL do relatório não encontrada nos XComs.")
        response = requests.get(report_location)
        if response.status_code == 200:
            report_data = response.json()
            kwargs['ti'].xcom_push(key='dsp_report_data', value=json.dumps(report_data))
            print("Dados do relatório baixados com sucesso.")
        else:
            raise Exception(f"Erro ao baixar relatório: {response.status_code} - {response.text}")