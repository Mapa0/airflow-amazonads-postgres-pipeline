import pandas as pd
import io
from datetime import datetime

class AmazonAdsAmcTransform:

    def __init__(self):
        pass

    def transform_csv_to_dataframe(self, **kwargs):
        # Recuperar o conteúdo bruto do CSV das XComs
        csv_content = kwargs['ti'].xcom_pull(task_ids='extract.extract_csv_content', key='raw_csv_content')

        if not csv_content:
            raise Exception("Conteúdo bruto do CSV não encontrado nas XComs.")

        try:
            # Converter o conteúdo bruto do CSV para um DataFrame
            df = pd.read_csv(io.StringIO(csv_content))
        except Exception as e:
            raise Exception(f"Erro ao carregar o CSV em um DataFrame: {e}")

        print("CSV convertido para DataFrame com sucesso.")

        # Remover linhas onde 'filtered' é True (se a coluna existir)
        if 'filtered' in df.columns:
            df = df[~df['filtered']]  # Mantém apenas linhas onde filtered é False

        # Dropar colunas 'du_count', 'true' e 'filtered' se existirem
        cols_to_drop = [col for col in ['du_count', 'true', 'filtered'] if col in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        # Adicionar colunas de contexto
        execution_date = datetime.now().strftime('%Y-%m-%d')
        time_window_start = kwargs['ti'].xcom_pull(task_ids='setup.set_parameters', key='timeWindowStart')
        time_window_end = kwargs['ti'].xcom_pull(task_ids='setup.set_parameters', key='timeWindowEnd')

        if not time_window_start or not time_window_end:
            raise Exception("Time Window Start ou End não encontrado nas XComs.")

        time_window_start_date = pd.to_datetime(time_window_start).strftime('%Y-%m-%d')
        time_window_end_date = pd.to_datetime(time_window_end).strftime('%Y-%m-%d')
        execution_date_date = pd.to_datetime(execution_date).strftime('%Y-%m-%d')

        df['execution_date'] = execution_date_date
        df['time_window_start'] = time_window_start_date
        df['time_window_end'] = time_window_end_date

        df = df.where(pd.notnull(df), None)
        df = self.fill_nulls_with_defaults(df)

        # Salvar DataFrame transformado nas XComs
        df_records = df.to_dict(orient='records')
        kwargs['ti'].xcom_push(key='csv_dataframe', value=df_records)
        print("DataFrame transformado salvo nas XComs.")

        return df
    
    def fill_nulls_with_defaults(self, df):
        for col in df.columns:
            if df[col].isnull().any():
                dtype = df[col].dtype
                if pd.api.types.is_numeric_dtype(dtype):
                    df[col] = df[col].fillna(0)
                elif pd.api.types.is_bool_dtype(dtype):
                    df[col] = df[col].fillna(False)
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    df[col] = df[col].fillna(pd.Timestamp('1970-01-01'))
                else:
                    # Assumindo que é uma coluna textual
                    df[col] = df[col].fillna('0')
        return df