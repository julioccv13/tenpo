import json
from core.ports import Process
import logging
import datetime
import time
from google.cloud import storage as st
import pandas
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
current_ds = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m%d')

class Process112_Send_Payloads(Process):
    def __init__(self, **kwargs):
        self._start_interval = None
        self._interval = kwargs["interval"]
        self._index = kwargs["index"]
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_target = kwargs["project_target"]
        self._custom_number = kwargs["custom_number"]
        self._clevertap_base_url = kwargs["clevertap_base_url"]
        self._clevertap_account_id = kwargs["clevertap_account_id"]
        self._clevertap_account_passcode = kwargs["clevertap_account_passcode"]
        self._common_headers = {
            "X-CleverTap-Account-Id": self._clevertap_account_id,
            "X-CleverTap-Passcode": self._clevertap_account_passcode,
            "Content-Type": "application/json"
        }
        
    def run(self):
         self._start_interval = self._calculate_init_interval()
         self._build_dataframes()
    
    @staticmethod
    def generar_lotes(df, tamano_lote=1000):
            inicio = 0
            fin = tamano_lote
            total_filas = len(df)
            while inicio < total_filas:
                yield df[inicio:fin]
                inicio = fin
                fin += tamano_lote

    def _calculate_init_interval(self):
        dataframe = pandas.read_gbq(f"""
            SELECT
                partition_id
            FROM `{self._project_target}.tmp.INFORMATION_SCHEMA.PARTITIONS`
            WHERE table_name LIKE '%table_final_{current_ds}_bloque_{self._index}_partitioned%'
            and partition_id not like '%__UNPARTITIONED__%'
            order by partition_id
            LIMIT 1
        """) 
        start_interval = int(dataframe.at[0,'partition_id'])
        return start_interval
      
    def _build_dataframes(self):
            
        if self._custom_number == 1:
            dataframe = pandas.read_gbq(f"""
            SELECT
                 identity,profileData
            from `{self._project_target}.tmp.table_final_{current_ds}_bloque_{self._index}_partitioned`
            where row 
            between {self._start_interval + 1}
            and {self._start_interval + (self._interval * self._custom_number)}
            """)
            self._send_payloads(dataframe)

        if self._custom_number == 2:
            dataframe = pandas.read_gbq(f"""
            SELECT
                 identity,profileData
            from `{self._project_target}.tmp.table_final_{current_ds}_bloque_{self._index}_partitioned`
            where row 
                between {self._start_interval + (self._interval * (self._custom_number-1)) + 1}
                and {self._start_interval + (self._interval * self._custom_number)}
            """)
            self._send_payloads(dataframe)
        
        if self._custom_number == 3:
            dataframe = pandas.read_gbq(f"""
            SELECT
                 identity,profileData
            from `{self._project_target}.tmp.table_final_{current_ds}_bloque_{self._index}_partitioned`
            where row 
            between {self._start_interval + (self._interval * (self._custom_number-1)) + 1}
            and {self._start_interval + (self._interval * self._custom_number)}
            """)
            self._send_payloads(dataframe)
                
        if self._custom_number == 4:
            dataframe = pandas.read_gbq(f"""
            SELECT
                 identity,profileData
            from `{self._project_target}.tmp.table_final_{current_ds}_bloque_{self._index}_partitioned`
            where row 
            between {self._start_interval + (self._interval * (self._custom_number-1)) + 1}
            and {self._start_interval + (self._interval * self._custom_number)}
            """)
            self._send_payloads(dataframe)
        
        if self._custom_number == 5:
            dataframe = pandas.read_gbq(f"""
            SELECT
                 identity,profileData
            from `{self._project_target}.tmp.table_final_{current_ds}_bloque_{self._index}_partitioned`
            where row 
            between {self._start_interval + (self._interval * (self._custom_number-1)) + 1}
            and {self._start_interval + (self._interval * self._custom_number)}
            """)
            self._send_payloads(dataframe)
    
    def _send_payloads(self, dataframe):

        def process_json(json_data):
            key_transform = dict()
            data = json.loads(json_data)
            return {key_transform.get(orig_key, orig_key): value for orig_key, value in data.items() if value is not None}

        dataframe['profileData'] = dataframe['profileData'].apply(process_json)
        dataframe['type'] = 'profile'
        lotes = Process112_Send_Payloads.generar_lotes(dataframe, tamano_lote=1000)
        for lote in lotes:
            subset_records = lote.to_dict(orient='records')
            payload = {"d": subset_records}
            try:
                response = requests.request("POST", f'{self._clevertap_base_url}1/upload', headers=self._common_headers, data=json.dumps(payload))
                response_text = response.text
                if ('success' or 'partial') not in str(response_text):
                    raise RuntimeError(f'An error ocurred: {str(response_text)}')
            except Exception as e:
                    self._logger_storage(f'{str(e)} of type: {type(e)}')
                    raise Exception(f'An error ocurred: {str(e)}')
        

    def _logger_storage(self, text:str):
        """This function helps us to log to a cloud storage and monitor the main script """
        from google.cloud import storage as st
        client = st.Client()
        bucket = client.get_bucket(self._bucket_name)
        bucket.blob(f'112/files/log_error_payload.txt').upload_from_string(text, 'text')
         
              


