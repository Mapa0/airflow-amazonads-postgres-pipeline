import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
import numpy as np

class AmazonAdsAmcLoad:

    def __init__(self):
        # Registrar o adapter para pd.NA
        register_adapter(type(pd.NA), self.adapt_pd_NA)
        # Registrar adaptadores para tipos NumPy, se necessário
        register_adapter(np.float64, self.adapt_numpy_float64)
        register_adapter(np.int64, self.adapt_numpy_int64)

    def adapt_pd_NA(self, val):
        return AsIs('NULL')

    def adapt_numpy_float64(self, val):
        return float(val)

    def adapt_numpy_int64(self, val):
        return int(val)

    def insert_data_incrementally_auto(self, **kwargs):

        db_config = {
            'host': os.getenv('DB_ENDPOINT'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT'),
        }

        df_records = kwargs['ti'].xcom_pull(task_ids='transform.transform_csv_to_dataframe', key='csv_dataframe')
        table_name = kwargs['ti'].xcom_pull(task_ids='setup.set_parameters', key='table_name')

        if not df_records or not table_name:
            raise Exception("DataFrame ou nome da tabela não encontrado no XCom.")

        # Recria o DataFrame a partir da lista de dicionários
        df = pd.DataFrame(df_records)

        # Forçar conversão de tipos mais precisos
        df = df.convert_dtypes()

        # Mapeamento de dtypes do Pandas para tipos do PostgreSQL
        dtype_map = {
            'Int64': 'BIGINT',
            'Float64': 'DOUBLE PRECISION',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'boolean': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP'
        }

        # Gerar lista de definições de colunas
        columns_definitions = []
        for col in df.columns:
            col_type = str(df[col].dtype)
            # Se não encontrar o tipo no mapeamento, usa TEXT por padrão.
            pg_type = dtype_map.get(col_type, 'TEXT')
            columns_definitions.append(f'"{col}" {pg_type}')

        # Preparar as colunas para criar tabela e primary keys (assumindo que todas as colunas são PK)
        primary_keys = [f'"{col}"' for col in df.columns]

        # Conexão com o banco
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns_definitions)},
                PRIMARY KEY ({', '.join(primary_keys)})
            );
        """
        print(f"Query de criação de tabela:\n{create_table_query}")
        cursor.execute(create_table_query)

        column_names = [f'"{col}"' for col in df.columns]
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(column_names)})
            VALUES %s
            ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE SET
            {', '.join([f'{col} = EXCLUDED.{col}' for col in column_names])};
        """
        print(f"Query de inserção/atualização:\n{insert_query}")

        # Converter dados do DF para lista de listas
        records = df.to_numpy().tolist()
        execute_values(cursor, insert_query, records)

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Dados inseridos ou atualizados na tabela '{table_name}' com sucesso.")