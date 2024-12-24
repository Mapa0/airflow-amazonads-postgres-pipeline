import time
import requests
import uuid
import os
from airflow.models import Variable
from dotenv import load_dotenv
from airflow.exceptions import AirflowException

class AmazonAdsAmcExtract:
    def __init__(self):
        load_dotenv()
        self.API_URL = os.getenv("AMAZON_ADS_AMC_API_URL")
        self.CLIENT_ID = os.getenv("AMAZON_ADS_CLIENT_ID")
        self.ADVERTISER_ID = os.getenv("AMAZON_ADS_API_ADVERTISER_ID")
        self.MARKETPLACE_ID = os.getenv("AMAZON_ADS_API_MARKETPLACE_ID")
        self.ACCESS_TOKEN = Variable.get("amazon_access_token")

    def create_amc_workflow(self, **kwargs):
        CREATE_WORKFLOW_URL = f"{self.API_URL}/workflows"
        workflow_id = str(uuid.uuid4())
        
        sql_query = kwargs['ti'].xcom_pull(task_ids='set_parameters', key='sql_query')

        headers = {
        "Authorization": f"Bearer {self.ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Amazon-Advertising-API-ClientID": self.CLIENT_ID,
        "Amazon-Advertising-API-AdvertiserId": self.ADVERTISER_ID,
        "Amazon-Advertising-API-MarketplaceId": self.MARKETPLACE_ID,
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

    def create_workflow_execution(self, **kwargs):
        execution_endpoint = f"{self.API_URL}/workflowExecutions"

        workflow_id = kwargs['ti'].xcom_pull(task_ids='create_amc_workflow', key='amc_workflow_id')
        time_window_start = kwargs['ti'].xcom_pull(task_ids='set_parameters', key='timeWindowStart')
        time_window_end = kwargs['ti'].xcom_pull(task_ids='set_parameters', key='timeWindowEnd')

        if not workflow_id:
            raise Exception("Workflow ID não encontrado. Certifique-se de que o workflow foi criado com sucesso.")

        headers = {
            "Authorization": f"Bearer {self.ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientID": self.CLIENT_ID,
            "Amazon-Advertising-API-AdvertiserId": self.ADVERTISER_ID,
            "Amazon-Advertising-API-MarketplaceId": self.MARKETPLACE_ID,
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

    def monitor_workflow_execution(self, **kwargs):
        headers = {
            "Authorization": f"Bearer {self.ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientID": self.CLIENT_ID,
            "Amazon-Advertising-API-AdvertiserId": self.ADVERTISER_ID,
            "Amazon-Advertising-API-MarketplaceId": self.MARKETPLACE_ID,
            "Accept": "application/vnd.amcworkflowexecutions.v1+json"
        }

        # Recupera o workflowExecutionId das XComs
        workflow_execution_id = kwargs['ti'].xcom_pull(task_ids='create_amc_workflow_execution', key='workflowExecutionId')

        if not workflow_execution_id:
            raise Exception("Workflow Execution ID não encontrado. Certifique-se de que a execução foi criada com sucesso.")

        execution_endpoint = f"{self.API_URL}/workflowExecutions/{workflow_execution_id}"

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

    def get_download_url(self, **kwargs):

        headers = {
            "Authorization": f"Bearer {self.ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientID": self.CLIENT_ID,
            "Amazon-Advertising-API-AdvertiserId": self.ADVERTISER_ID,
            "Amazon-Advertising-API-MarketplaceId": self.MARKETPLACE_ID,
            "Accept": "application/vnd.amcworkflows.v1+json"
        }

        workflow_execution_id = kwargs['ti'].xcom_pull(task_ids='create_amc_workflow_execution', key='workflowExecutionId')
        if not workflow_execution_id:
            raise AirflowException("Workflow Execution ID não encontrado. Certifique-se de que a execução foi bem-sucedida.")

        download_url_endpoint = f"{self.API_URL}/workflowExecutions/{workflow_execution_id}/downloadUrls"

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
        
    def extract_csv_content(self, **kwargs):
        # Recuperar URLs de download das XComs
        download_urls = kwargs['ti'].xcom_pull(task_ids='get_download_url', key='download_urls')

        if not download_urls or len(download_urls) == 0:
            raise Exception("Nenhuma URL de download encontrada nas XComs.")

        download_url = download_urls[0]
        print(f"Baixando dados de: {download_url}")

        response = requests.get(download_url)
        if response.status_code != 200:
            raise Exception(f"Erro ao obter o CSV: {response.status_code} - {response.text}")

        # Salvar conteúdo bruto do CSV nas XComs
        csv_content = response.text
        kwargs['ti'].xcom_push(key='raw_csv_content', value=csv_content)
        print("Conteúdo bruto do CSV salvo nas XComs.")

        return csv_content
