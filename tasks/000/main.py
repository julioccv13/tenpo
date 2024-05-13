from core.ports import Process
import time
from google.cloud import storage as st
import psycopg2

class ingest_postgres(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_target = kwargs["project_target"]
        self._host =  kwargs["host"]
        self._user =  kwargs["user"]
        self._password =  kwargs["password"]
        self._database = kwargs["database"]
    
    def run(self):
        self._connect_postgres()
  
    def _connect_postgres(self):
        connection = psycopg2.connect(
                dbname=self._database,
                user=self._user,
                password=self._password,
                host=self._host
            )
        cursor = connection.cursor()

        query_information_table = '''   
            SELECT 
                table_schema,table_name,column_name,data_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME='campaign'
            LIMIT 100
            '''
        cursor.execute(query_information_table)
        results = cursor.fetchall()
        print(results)

    