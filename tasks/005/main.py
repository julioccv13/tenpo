import argparse
import requests
import unidecode
import pandas as pd
from core.ports import Process
from core.google import storage, bigquery
from datetime import datetime




INSERT_PATH = "gs://{bucket_name}/005/insert.sql"
URL = "https://ayuda.tenpo.cl/reports/scheduled_exports/7525011679517483/download_file.json?uuid=231c6996-45fc-4266-b50f-294ac11eb406"



class Process005(Process):
    
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_source = kwargs["project_source"]
        self._project_target = kwargs["project_target"]
        self._api_key = kwargs["api_key"]
        
    def run(self):
        self._get_daily_tickets()
        self._generate_insert()

             
    def _generate_insert(self):
        self._execute_query(path=INSERT_PATH)
    
    def _get_daily_tickets(self):
        headers = {'Content-type': 'application/json'}
        response = requests.get(URL, headers=headers,auth=(self._api_key, 'X'))
        if response.status_code==200:
            output = response.json()['export']['url']
            data = pd.read_csv(output, dtype='str')
            data = self._structure_data_sac(df = data)
            table_name =f"temp.daily_tickets_{self._ds_nodash}"          
            data.to_gbq(table_name, project_id=self._project_source, if_exists="replace")
            
    
    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)
    
    def _structure_data_sac(self, df:pd.DataFrame):

        df["Association_type"] = df["Last_assigned_date"] =  df["First_assigned_date"] = df["No_of_group_reassigns_till_date"]  = None
        df["Agent_language"] = df["Agent_timezone"] =  df["Assign_type"] = df["First_assigned_group"]  = None
        df["Last_updated_agent"] = df["Agent_timezone"] =  df["Assign_type"] = df["First_assigned_group"]  = None
        df["First_assigned_agent"] = df["Agent_email"] =  df["Tickets_Age"] = df["First_response_group"]  = None
        df["First_response_agent"] = df["Agent_timezone"] =  df["Assign_type"] = df["First_assigned_group"]  = None
        df["Booking_ID_Created_date"] = df["Booking_ID_Updated_date"] =  df["fdticket_Created_date"] = df["fdticket_Updated_date"]  = None
        df["Booking_ID_Created_date1"] = df["Booking_ID_Updated_date1"] =  df["Item__Created_date"] = df["Item__Updated_date"]  = None
        df["Search_Agent_Created_date"] = df["Search_Agent_Updated_date"] =  df["Booking_ID_Created_date2"] = df["Booking_ID_Updated_date2"]  = None

        df.columns = map(str.lower, df.columns)
        
        df_output = df[['id del ticket',
                        'agente',
                        'estado',
                        'prioridad',
                        'fuente',
                        'association_type',
                        'interacciones del cliente',
                        'acción',
                        'hora de resolución',
                        'last_assigned_date',
                        'fecha transacción',
                        'hora de cierre',
                        'first_assigned_date',
                        'tipo de transacción',
                        'asunto',
                        'subtipificación del caso',
                        'intercciones del agente',
                        'hora de última actualizacion',
                        'hora creación',
                        'no_of_group_reassigns_till_date',
                        'tiempo inicial de respuesta',
                        'tipo',
                        'agent_language',
                        'agent_timezone',
                        'assign_type',
                        'producto',
                        'internal group',
                        'first_assigned_group',
                        'last_updated_agent',
                        'internal agent',
                        'first_assigned_agent',
                        'agent_email',
                        'tickets_age',
                        'first_response_group',
                        'first_response_agent',
                        'id cola',
                        'correo',
                        'id trx origen',
                        'etiquetas',
                        'booking_id_created_date',
                        'booking_id_updated_date',
                        'fdticket_created_date',
                        'fdticket_updated_date',
                        'booking_id_created_date1',
                        'booking_id_updated_date1',
                        'item__created_date',
                        'item__updated_date',
                        'search_agent_created_date',
                        'search_agent_updated_date',
                        'booking_id_created_date2',
                        'booking_id_updated_date2']]
        
 
        df_output.columns = ['Ticket_ID',
                            'Agent_name',
                            'Status',
                            'Priority',
                            'Source',
                            'Association_type',
                            'Customer_reply_count',
                            'Accion',
                            'Resolved_date',
                            'Last_assigned_date',
                            'Fecha_transaccion',
                            'Closed_date',
                            'First_assigned_date',
                            'Tipo_de_Transaccion',
                            'Subject',
                            'Subtipificacion_del_caso',
                            'No_of_agent_reassigns_till_date',
                            'Last_updated_date',
                            'Created_date',
                            'No_of_group_reassigns_till_date',
                            'First_response_date',
                            'Ticket_type',
                            'Agent_language',
                            'Agent_timezone',
                            'Assign_type',
                            'Product',
                            'Internal_group_name',
                            'First_assigned_group',
                            'Last_updated_agent',
                            'Internal_agent_name',
                            'First_assigned_agent',
                            'Agent_email',
                            'Tickets_Age',
                            'First_response_group',
                            'First_response_agent',
                            'ID_cola',
                            'Correo',
                            'ID_Trx_Origen',
                            'Tag_name',
                            'Booking_ID_Created_date',
                            'Booking_ID_Updated_date',
                            'fdticket_Created_date',
                            'fdticket_Updated_date',
                            'Booking_ID_Created_date1',
                            'Booking_ID_Updated_date1',
                            'Item__Created_date',
                            'Item__Updated_date',
                            'Search_Agent_Created_date',
                            'Search_Agent_Updated_date',
                            'Booking_ID_Created_date2',
                            'Booking_ID_Updated_date2']
        
        to_string_list = ["Ticket_ID","Customer_reply_count","No_of_agent_reassigns_till_date","ID_cola","Correo","ID_Trx_Origen"]
        for x in to_string_list:
            df_output[x] = df_output[x].apply(str)
        df_output["execution_date"] = pd.Series([((pd.Timestamp(datetime.now())).round('1d')).to_pydatetime()] * df_output.shape[0])
        df_output['Created_date'] = (pd.to_datetime(df_output['Created_date']).dt.strftime('%Y-%m-%d %I:%M:%S %p'))
        df_output['First_response_date'] = (pd.to_datetime(df_output['First_response_date']).dt.strftime('%Y-%m-%d %I:%M:%S %p'))
        df_output['Last_updated_date'] = (pd.to_datetime(df_output['Last_updated_date']).dt.strftime('%Y-%m-%d %I:%M:%S %p'))
        df_output['Resolved_date'] = (pd.to_datetime(df_output['Resolved_date']).dt.strftime('%Y-%m-%d %I:%M:%S %p'))
        df_output['Closed_date'] = (pd.to_datetime(df_output['Closed_date']).dt.strftime('%Y-%m-%d %I:%M:%S %p'))

        return df_output

    def _parse_query(self, query):
        return (query
                .replace(r"{{ds_nodash}}", self._ds_nodash)
                .replace(r"${project_source}", self._project_source)
                .replace(r"${project_target}", self._project_target)
                .format(**{"project_id": self._project_id})
                )

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-D",
                        "--ds_nodash",
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
    parser.add_argument("-PS",
                        "--project_source",
                        type=str,
                        required=True)
    parser.add_argument("-PT",
                        "--project_target",
                        type=str,
                        required=True)
    parser.add_argument("-AK",
                    "--api_key",
                    type=str,
                    required=True)


    args = parser.parse_args()
    return vars(args)

if __name__ == '__main__':
    args = get_args()
    process = Process005(**args)
    process.run()