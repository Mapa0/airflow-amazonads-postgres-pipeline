from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import os

# Configurações gerais do OAuth 2 - utilizando variáveis de ambiente
CLIENT_ID = os.getenv("AMAZON_ADS_CLIENT_ID")
CLIENT_SECRET = os.getenv("AMAZON_ADS_CLIENT_SECRET")
TOKEN_URL = os.getenv("AMAZON_ADS_TOKEN_URL")
REDIRECT_URI = os.getenv("AMAZON_ADS_REDIRECT_URI")
REFRESH_TOKEN = os.getenv("AMAZON_ADS_REFRESH_TOKEN")

# Funções de autenticação
def save_token_to_variable(token_data):
    Variable.set("amazon_access_token", token_data["access_token"])
    Variable.set("amazon_refresh_token", token_data.get("refresh_token", REFRESH_TOKEN))
    Variable.set("token_expiration", token_data["expires_in"] + int(datetime.now().timestamp()))

def load_token_from_variable():
    access_token = Variable.get("amazon_access_token", default_var=None)
    refresh_token = Variable.get("amazon_refresh_token", default_var=REFRESH_TOKEN)
    expiration = int(Variable.get("token_expiration", default_var="0"))
    return {"access_token": access_token, "refresh_token": refresh_token, "expires_in": expiration}

def refresh_access_token():
    token_data = load_token_from_variable()
    data = {
        "grant_type": "refresh_token",
        "refresh_token": token_data["refresh_token"],
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    response = requests.post(TOKEN_URL, data=data)
    if response.status_code == 200:
        new_token_data = response.json()
        save_token_to_variable(new_token_data)
        print("Token renovado com sucesso:", new_token_data["access_token"])
    else:
        raise Exception(f"Erro ao renovar o token: {response.status_code} - {response.text}")

def get_valid_access_token():
    token_data = load_token_from_variable()
    current_time = int(datetime.now().timestamp())
    
    if current_time >= token_data["expires_in"] - 60:
        print("Token expirado ou próximo de expirar. Renovando...")
        refresh_access_token()
    else:
        print("Token ainda é válido:", token_data["access_token"])

# Configurações da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="amazon_ads_authentication",  # Nome da DAG
    default_args=default_args,
    description="Autenticação no Amazon Ads API com renovação de token",
    schedule_interval=None,  # Executa apenas quando acionada
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["amazon", "authentication"],
) as dag:

    # Tarefa única: Verificar e renovar o token, se necessário
    check_and_renew_token = PythonOperator(
        task_id="check_and_renew_token",
        python_callable=get_valid_access_token,
    )