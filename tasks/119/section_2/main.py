from core.google import bigquery, storage
from core.ports import Process
import logging
import datetime
import pandas
import requests
import json
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
current_ds = datetime.date.fromordinal(datetime.date.today().toordinal()).strftime('%Y-%m-%d')
yesterday_ds = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m%d')

class Process119_Send_Payloads(Process):
    def __init__(self, **kwargs):
        self._ds_nodash = yesterday_ds
        self._bucket_name = kwargs["bucket_name"]
        self._ga_base_url = kwargs["ga_base_url"]
        self._firebase_app_id = kwargs["firebase_app_id"]
        self._ios_id = kwargs["ios_id"]
        self._android_id = kwargs["android_id"]
        self._ios_api_secret = kwargs["ios_api_secret"]
        self._android_api_secret = kwargs["android_api_secret"]
        self._env = kwargs["env"]

    def run(self):
        data = self._read_dataframe()
        ios_event_list, androind_event_list = self._create_split_dataframes(data)
        self._send_payloads(ios_event_list,androind_event_list)
        
    def _read_dataframe(self):
        dataframe = pandas.read_gbq(f"""
            SELECT * 
            FROM `tenpo-datalake-{self._env}.jarvis.event_string_query`
            WHERE user IN 
                (
                    SELECT 
                        DISTINCT user 
                    FROM `tenpo-datalake-{self._env}.jarvis.target_event_string_query`
                )
            """)
        return dataframe

    def _create_json_list(self, df_data):
        json_list = []
        for _, row in df_data.iterrows():
            json_data = {
                "app_instance_id": row["pseudo_user_id"],
                "user_id": row["user"],
                "non_personalized_ads": False,
                "user_properties":{
                    "edad":{
                        "value": str(row["edad"])},
                    "genero":{
                        "value": row["genero"]},
                    "tipo_mau_mes":{
                        "value": row["tipo_mau_mes"]},
                    "segmento_cliente":{
                        "value": row["segmento_cliente"]},
                    "producto_top1_historico":{
                        "value": row["producto_top1_historico"]},
                    "productos_activos_mes":{
                        "value": row["productos_activos_mes_actual"]},
                    "producto_a_activar":{
                        "value": row["producto_a_activar"]},
                    "producto_a_reactivar":{
                        "value": row["producto_a_reactivar"]},
                    "producto_me_gusta":{
                        "value": row["producto_me_gusta"]},
                    "producto_me_encanta":{
                        "value": row["producto_me_encanta"]},
                    "producto_no_es_para_mi":{
                        "value": row["producto_no_es_para_mi"]}
                    },
                "events": [
                    {
                        "name": "properties_jarvis_S2S",
                        "params": {
                            "edad": str(row["edad"]),
                            "genero": row["tipo_mau_mes"],
                            "tipo_mau_mes": row["tipo_mau_mes"],
                            "segmento_cliente": row["segmento_cliente"],
                            "producto_top1_historico": row["producto_top1_historico"],
                            "productos_activos_mes": row["productos_activos_mes_actual"],
                            "producto_a_activar": row["producto_a_activar"],
                            "producto_a_reactivar": row["producto_a_reactivar"],
                            "producto_me_gusta": row["producto_me_gusta"],
                            "producto_me_encanta": row["producto_me_encanta"],
                            "producto_no_es_para_mi": row["producto_no_es_para_mi"]
                            }
                        }
                    ]
            }
            json_list.append(json_data)
        return json_list
    
    def _create_split_dataframes(self,data):
        df_ios_data = data[data["operating_system"] == "iOS"]
        df_android_data = data[data["operating_system"] == "Android"]
        ios_event_list = self._create_json_list(df_ios_data)
        android_event_list = self._create_json_list(df_android_data)
        return ios_event_list, android_event_list
    
    def _send_data_ga4(self,event_list, url):
        import ssl
        try:
            list_for_users_send = []
            counter: int = 0
            response = None
            for item in event_list:
                ssl._create_default_https_context = ssl._create_unverified_context
                response = requests.post(url,data=json.dumps(item),verify=False)
                list_for_users_send.append(item["user_id"])
                counter += 1
            return response, counter, list_for_users_send

        except Exception as e:
            raise Exception(f"Error trying to send events to GA4: {str(e)}")
    
    def _send_payloads(self, ios_data, android_data):
        response_ios = self._send_data_ga4(ios_data, f'{self._ga_base_url}={self._firebase_app_id}:ios:{self._ios_id}&api_secret={self._ios_api_secret}')
        response_android = self._send_data_ga4(android_data, f'{self._ga_base_url}={self._firebase_app_id}:android:{self._android_id}&api_secret={self._android_api_secret}')
        if len(response_ios[2]) != len(ios_data):
            raise Exception('Faltan usuarios de Ios')
        else:
            self._logger_storage(f'Se cargaron {str(response_ios[1])} de IOS con fecha de carga {str(current_ds)}','ios')

        if len(response_android[2]) != len(android_data):
            raise Exception('Faltan usuarios de Android')
        else:
            self._logger_storage(f'Se cargaron {str(response_android[1])} de Android con fecha de carga {str(current_ds)}','android')
        
    def _logger_storage(self, text:str, os):
        from google.cloud import storage as st
        client = st.Client()
        bucket = client.get_bucket(self._bucket_name)
        bucket.blob(f'119/files/log_{os}.txt').upload_from_string(text, 'text')