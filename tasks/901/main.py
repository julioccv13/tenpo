import argparse
import pandas as pd
import json
from core.ports import Process
from core.google import storage, bigquery
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail



QUERY_PATH_1 = "gs://{bucket_name}/901/queries/01_get_cases.sql"
QUERY_PATH_2 = "gs://{bucket_name}/901/queries/02_save_sent_cases.sql"

class Process901(Process):
    
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ts_nodash = kwargs["ts_nodash"]
        self._ts = kwargs["ts"]
        self._project_id = kwargs["project_id"]
        self._sendgrid_email_from = kwargs["sendgrid_email_from"]
        self._sendgrid_api_key = kwargs["sendgrid_api_key"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_target = kwargs["project_target"]
    
    def run(self):
        self._generate_query_1()
        self._generate_query_2()
        # self._send_email_dof()
        
    def _generate_query_1(self):
        self._execute_query(path=QUERY_PATH_1)

    def _generate_query_2(self):
        self._execute_query(path=QUERY_PATH_2)    
    
    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)

    def _parse_query(self, query):
        return (query
                .replace(r"{{ts_nodash}}", self._ts_nodash)
                .replace(r"${project_source_1}", self._project_source_1)
                .replace(r"${project_source_2}", self._project_source_2)
                .replace(r"${project_target}", self._project_target)
                .format(**{"project_id": self._project_id})
                )
    def _send_email_dof(self):
        

        # Podria ser mas eficiente hacerlo una sola vez
        query = f"SELECT rut, nombres, apellidos, fecha_hora_creacion, monto_dolares, monto_pesos, tipo FROM {self._project_target}.temp.p_dof_{self._ts_nodash} ORDER BY rut"
        data = pd.read_gbq(
            query, project_id=self._project_target)

        html_table = data.to_html()

        if len(data) != 0:
            html_content = """
                <h1> Proceso DOF </h1> 
                
                <p>Se adjunta tabla de nuevos casos. En caso de problemas contactar a jarvis@tenpo.cl.</p>
                %s
            """ % html_table
            subject = f"Casos Proceso DOF - {self._ts}"
            
            
            message = Mail(from_email=self._sendgrid_email_from,
                        to_emails=["jarvis@tenpo.cl", "cumplimiento_interno@tenpo.cl"],
                        subject=subject,
                        html_content=html_content)            
            
            json_data = json.loads(self._sendgrid_api_key)
            api_key = json_data['sendgrid_api_key']    
            sg = SendGridAPIClient(api_key)
            response = sg.send(message)
  

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-T",
                        "--ts_nodash",
                        type=str,
                        required=True)
    parser.add_argument("-TS",
                        "--ts",
                        type=str,
                        required=True)
    parser.add_argument("-BN",
                        "--bucket_name",
                        type=str,
                        required=True)
    parser.add_argument("-PI",
                        "--project_id",
                        type=str,
                        required=True)
    parser.add_argument("-PS1",
                        "--project_source_1",
                        type=str,
                        required=True)
    parser.add_argument("-PS2",
                        "--project_source_2",
                        type=str,
                        required=True)
    parser.add_argument("-PT",
                        "--project_target",
                        type=str,
                        required=True)
    parser.add_argument("-SAK",
                    "--sendgrid_api_key",
                    type=str,
                    required=True)
    parser.add_argument("-SEF",
                    "--sendgrid_email_from",
                    type=str,
                    required=True)


    args = parser.parse_args()
    return vars(args)

if __name__ == '__main__':
    args = get_args()
    process = Process901(**args)
    process.run()