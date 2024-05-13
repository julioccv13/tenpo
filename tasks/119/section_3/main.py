from core.ports import Process
import logging
import datetime
import pandas
import requests
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
current_ds = datetime.date.fromordinal(datetime.date.today().toordinal()).strftime('%Y-%m-%d')
GA4_IOS_URL = "https://www.google-analytics.com/mp/collect?firebase_app_id=1:55490484269:ios:2ff50c452ffdaa5f53c0a4&api_secret=7ACQoqNhQreTLmrMno6zJg"
GA4_ANDROID_URL = "https://www.google-analytics.com/mp/collect?firebase_app_id=1:55490484269:android:7c0aa8bf25a6490653c0a4&api_secret=O_USr_9zTnG7imjTYhHBYA"

class Process119_Bulk_Upload(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]

    @staticmethod
    def split_dataframe(df, chunk_size = 25000): 
        num_chunks = len(df) // chunk_size + 1
        for i in range(num_chunks):
            yield df[i*chunk_size:(i+1)*chunk_size]
    
    def run(self):
        data = self._read_dataframe()
        self._create_split_dataframes_send_to_ga4(data)

    def _read_dataframe(self):
        dataframe = pandas.read_gbq(f"""
            SELECT * 
            FROM `tenpo-datalake-sandbox.jarvis.event_string_query`
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
    
    def _create_split_dataframes_send_to_ga4(self,data):
        """Yield over the entire dataframe"""        
        dataframes = Process119_Bulk_Upload.split_dataframe(data)
        for data in dataframes:
            ios_event_list = self._create_json_list(data[data["operating_system"] == "iOS"])
            android_event_list = self._create_json_list(data[data["operating_system"] == "Android"])
            self._send_payloads(ios_event_list,android_event_list)
    
    def _send_payloads(self, ios_data, android_data):
        response_ios = self._send_data_ga4(ios_data,GA4_IOS_URL)
        response_android = self._send_data_ga4(android_data,GA4_ANDROID_URL)
        if len(response_ios[2]) != len(ios_data):
            raise Exception('Faltan usuarios de Ios')
        else:
            self._logger_storage(f'Se cargaron {str(response_ios[1])} de IOS con fecha de carga {str(current_ds)}','ios')

        if len(response_android[2]) != len(android_data):
            raise Exception('Faltan usuarios de Android')
        else:
            self._logger_storage(f'Se cargaron {str(response_android[1])} de Android con fecha de carga {str(current_ds)}','android')
    
    def _send_data_ga4(self,event_list, url):
        import ssl
        try:
            list_for_users_send = []
            counter: int = 0
            for item in event_list:
                ssl._create_default_https_context = ssl._create_unverified_context
                response = requests.post(url,data=json.dumps(item),verify=False)
                list_for_users_send.append(item["user_id"])
                counter += 1
            return response, counter, list_for_users_send

        except Exception as e:
            raise Exception(f"Error trying to send events to GA4: {str(e)}")
        
    def _logger_storage(self, text:str, os):
        from google.cloud import storage as st
        client = st.Client()
        bucket = client.get_bucket(self._bucket_name)
        bucket.blob(f'119/files_bulk_upload/log_{os}.txt').upload_from_string(text, 'text')