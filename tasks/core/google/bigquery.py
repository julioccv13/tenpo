from typing import Optional
from google.cloud import bigquery
import pandas as pd
from google.cloud.exceptions import NotFound

client = bigquery.Client()


def execute_query(query: str):
    job = client.query(query)
    return job.result()


def get_query_result(query: str):
    return execute_query(query).result()


def get_one_row_result(query: str):
    return get_query_result(query)[0]

def insert_rows(table_id:str,df):
    job = client.load_table_from_dataframe(
        df, table_id
    ) 
    print("insert rows succesfull")

def insert_rows_append(table_id:str,df):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    ) 
    print("insert rows succesfull")

def get_bq_to_dataframe(query_data:str):
    df = client.query(query_data).result().to_dataframe()
    return df

def reload_table(client1, table_id, schema, df):
    try:
        job_config = bigquery.LoadJobConfig()
        job_config.schema = schema
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f'Tabla {table_id} cargada correctamente.')

    except NotFound:
        # La tabla no existe, por lo tanto, creamos la tabla a partir del DataFrame df
        print(f"La tabla {table_id} no existe en BigQuery. Creando nueva tabla.")
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f'Tabla {table_id} creada y cargada correctamente.')
    except Exception as e:
        # Manejo de cualquier otra excepción
        print(f"Ocurrió un error al cargar la tabla: {e}")

def upload_to_bigquery_stage_file(
    table_id: str,
    file_path: str,
    create_disposition: Optional[str] = "CREATE_IF_NEEDED",
    write_disposition: Optional[str] = "WRITE_TRUNCATE",
):

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.create_disposition = create_disposition
    job_config.write_disposition = write_disposition
    job_config.autodetect = True

    load_job = client.load_table_from_uri(file_path, table_id, job_config=job_config)
    load_job.result()

