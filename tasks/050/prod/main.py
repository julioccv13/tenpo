from core.ports import Process
import requests
import logging

class AutomaticCashback(Process):
    def __init__(self, **kwargs):
        self._bucket_path = kwargs["bucket_path"]
        self._campaing_url = kwargs["campaing_url"]
        self._campaign_id = kwargs["campaign_id"]
        self._segment_name = kwargs["segment_name"]
        self._headers = {'Content-Type': 'application/json'}
    
    def run(self):
        self._send_petition()
       
    def _send_petition(self):
        request = requests.post(self._campaing_url, data=self._get_body(), headers=self._headers)
        if request.status_code == 200:
            logging.info(f'Response text: {request.text} and Status Code: {request.status_code}')
        elif request.status_code == 400 and 'No se ha obtuvo resultados en el segmento' in request.text:
            logging.info(f'Response text: {request.text}, Response Reason: {request.reason} and Status Code: {request.status_code}')
        elif request.status_code == 404 and 'No se ha obtuvo resultados en el segmento' in request.text:
            logging.info(f'Response text: {request.text}, Response Reason: {request.reason} and Status Code: {request.status_code}')
        elif request.status_code >= 499:
            raise Exception(f"Error de comunicacion con el servicio de campañas: {request.text}")
        else:
            raise Exception(f'An error ocurred: {request.text} with status code: {request.status_code}')
    
    def _get_body(self) -> str:
        str_body = '(\"campaign_id\":\"{campaign_id}\",\"segment_sql_file_path\":\"gs://{bucket_path}/{segment_name}\")'.format(campaign_id=self._campaign_id,bucket_path=self._bucket_path,segment_name=self._segment_name)
        body = str_body.replace("(","{").replace(")","}")
        return body