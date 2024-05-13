import json
import pandas as pd
import requests
import pyarrow
import datetime
import time
import logging
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta

from core.google import bigquery, storage
from core.ports import Process
from core.spark import Spark

CREATE_QUERY_PATH = "gs://{bucket_name}/903_QA_base_monitoring/queries/01_transform_data.sql"


class ProcessQABase(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._project = kwargs["project"] # tenpo-datalake-prod
        self._host =  kwargs["host"]
        self._user =  kwargs["user"]
        self._password =  kwargs["password"]
        self._database = kwargs["database"]
        self._execution_date = pd.Timestamp.now()
        
    def run(self):
        #TODO: consultar las credenciales de USERS UAT
        self.run_test_uat_query()
        # self._qa_quantity()

    # Functión que nos entrega el dia de ayer en epoch.
    def run_test_uat_query(self):
        pg_schema = 'public'
        pg_table = 'campaign'
        #TODO: revisar como agregar estas conexiones sin que esten visibiles -> probar con el secret manager de Argo
        #TODO: simular la prueba de calidad en sanbox
        #TODO: validar conexion de campaign_manager en prod -> Adri
        engine = create_engine(f'postgresql://{self._user}:{self._password}@{self._host}:5432/{self._database}',
                            connect_args={'sslmode': 'require'})
            
        queryPg = f"""
                SELECT 1
                FROM {pg_schema}.{pg_table}
                LIMIT 1
            """
        resultsPg = pd.read_sql_query(queryPg, engine)
        engine.dispose()

    def _qa_quantity(self):
        current_time = datetime.datetime.now()
        target_date = current_time - datetime.timedelta(days=30)
        month_before = (target_date - relativedelta(months = 1)).strftime("%Y-%m")
        execution_date = current_time.strftime("%Y-%m-%d")

        #Variables insert
        project_id = self._project
        dataset_name = "data_governance"
        table_name = "qa_tables_audit_quantity"        
        #clientProd = bigquery.Client(project=project_id)
        QUERYvars = f"""
                SELECT * FROM `{project_id}.{dataset_name}.qa_tables_master` 
            """
        dfVars = (
                    bigquery.get_bq_project_to_dataframe(query_data=QUERYvars,project_id=project_id)
                )

        for index, row in dfVars.iterrows():
            conn_id = row['pg_connection_id']
            bq_proj_id = row['bq_project_id']
            bq_schema = row['bq_schema']
            bq_table = row['bq_table']
            date_grouping_field = row['date_grouping_field']
            pg_select_statement = row['pg_select_statement']
            pg_schema = row['pg_schema']
            pg_table = row['pg_table']
            
            #Postgres Connection
            engine = create_engine('sshh 🙈',
                            connect_args={'sslmode': 'require'})
            
            queryPg = f"""
                SELECT TO_CHAR({date_grouping_field},'YYYY-mm') as fecha, COUNT(*) as total
                FROM {pg_schema}.{pg_table}
                where TO_CHAR({date_grouping_field},'YYYY-mm') <= '{month_before}'
                group by TO_CHAR({date_grouping_field},'YYYY-mm')

            """
            
            resultsPg = pd.read_sql_query(queryPg, engine)
            # Close the database connection
            engine.dispose()
            
            
            #clientBq = bigquery.Client(project=f"{bq_proj_id}")
            #Big Query
            QUERY = f"""
                    SELECT FORMAT_DATE("%Y-%m", {date_grouping_field}) as fecha, COUNT(*) as total
                    FROM {bq_schema}.{bq_table}
                    where FORMAT_DATE("%Y-%m", {date_grouping_field}) <= '{month_before}'
                    group by FORMAT_DATE("%Y-%m", {date_grouping_field})
                """

            dfBq = (
                        bigquery.get_bq_project_to_dataframe(query_data=QUERY,project_id=bq_proj_id)
                    )
            
            dfMerged = resultsPg.merge(dfBq, on='fecha', suffixes=('_pg', '_bq'))
            
            dfMerged.insert(0, 'table_name_pg',pg_table)
            dfMerged.insert(0, 'table_schema_pg',pg_schema)
            dfMerged.insert(0, 'conn_id_pg',conn_id)
            dfMerged.insert(0, 'table_name_bq',bq_table)
            dfMerged.insert(0, 'table_schema_bq',bq_schema)
            dfMerged.insert(0, 'project_id',bq_proj_id)
            dfMerged["execution_date"] = execution_date
            dfMerged['has_differences_for_delete'] = dfMerged['total_bq'] - dfMerged['total_pg'] > 0
            dfMerged['has_differences_for_insert'] = dfMerged['total_pg'] - dfMerged['total_bq'] > 0
            
            delete_query = f"""
                DELETE FROM `{project_id}.{dataset_name}.{table_name}`
                WHERE execution_date = "{execution_date}" and project_id = "{bq_proj_id}" and table_schema_bq = "{bq_schema}" and table_name_bq = "{bq_table}"
            """

            # Submit the delete query
            #job = clientProd.query(delete_query)
            #job.result()
            bigquery.execute_query(query=delete_query)
            
            #table_ref = clientProd.dataset(dataset_name).table(table_name)
            #job_config = bigquery.LoadJobConfig()
            #job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

            #clientProd.load_table_from_dataframe(dfMerged, table_ref, job_config=job_config).result()
            bigquery.insert_rows_append_with_ref(table_name=table_name,dataset_name=dataset_name,df=dfMerged)
            
            del resultsPg
            del dfBq
            del dfMerged