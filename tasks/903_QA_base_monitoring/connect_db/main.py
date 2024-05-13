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
import psycopg2


class ProcessQABase(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._project_id = kwargs["project_id"]
        self._host =  kwargs["host"]
        self._user =  kwargs["user"]
        self._password =  kwargs["password"]
        self._database = kwargs["database"]
        self._table_name = kwargs["table_name"]
    
    def run(self):
        self._connectDb()
    
    def _connectDb(self):
        connection = psycopg2.connect(
            dbname =self._database,
            user = self._user,
            password = self._password,
            host = self._host 
        )
        cursor = connection.cursor()
        query_information_table = f'''
            SELECT 
                table_schema,table_name,column_name,data_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{self._table_name}'
            LIMIT 100
            '''
        cursor.execute(query_information_table)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns =['database', 'table', 'column','type'])
        df.to_gbq(f'db_schemas.{self._table_name}', project_id=self._project_id, if_exists="replace")