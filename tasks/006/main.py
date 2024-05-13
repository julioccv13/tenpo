from datetime import datetime as date_time
import os
import pandas as pd
import pysftp
from core.google import bigquery, storage
from core.ports import Process
import datetime

INSERT_PATH = "gs://{bucket_name}/006/insert.sql"
ds_nodedash = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m%d')
ds = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y-%m-%d')
current_year_month = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m')

class Process006(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._ds = kwargs["ds"]
        self._project_id = kwargs["project_id"]
        self._project_source = kwargs["project_source"]
        self._project_target = kwargs["project_target"]
        self._host_sftp = kwargs["host_sftp"]
        self._username_sftp = kwargs["username_sftp"]
        self._password_sftp = kwargs["password_sftp"]

    def run(self):
        self._connect_sftp()
        self._report_process()
        self._generate_insert()
        self._remove_file()

    def _connect_sftp(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(
            self._host_sftp, username=self._username_sftp, password=self._password_sftp, cnopts=cnopts
        ) as sftp:
            with sftp.cd("/Data/Reportes"):
                files = sftp.listdir('/Data/Reportes')
                for file in files:
                    """ The tab at the end it's intentional """
                    if 'Reporte Tenpo IVR Gestiones ' in file:
                        sftp.get(file)
    @staticmethod
    def listdir(remote_path):
        """lists all the files and directories in the specified path and returns them"""
        for obj in pysftp.Connection.listdir(remote_path):
            yield obj

    def _report_process(self):
        files_dir = os.listdir()
        for file in files_dir:
            if 'Reporte Tenpo IVR Gestiones' in file:
                df = pd.read_excel(file, header=0, dtype="str")
                df["Fecha Larga"] = df["Fecha Larga"].apply(
                    lambda x: date_time.strptime(x, "%d-%m-%Y %H:%M:%S")
                )
                df["fecha_larga_day"] = df["Fecha Larga"].apply(
                        lambda x: date_time.strftime(x, "%Y-%m-%d")
                )
                report_datetime = date_time.strptime(f'{df["fecha_larga_day"][0]}','%Y-%m-%d').strftime('%Y%m')
                df = df.rename(
                        columns={
                            "Llamadas recibidas": "llamadas_recibidas",
                            "Llamadas atendidas": "llamadas_atendidas",
                            "RUT": "rut",
                            "Opcion de menu": "opcion_del_menu",
                            "Fecha Larga": "fecha_larga",
                            "ANI": "ani",
                            "TMO": "tmo",
                            "EJECUTIVO": "ejecutivo",
                            "PREGUNTA1": "respuesta_pregunta_1",
                            "PREGUNTA2": "respuesta_pregunta_2",
                            "DEFINICION": "definicion",
                        }
                    )

                check_columns = [
                        "llamadas_recibidas",
                        "llamadas_atendidas",
                        "rut",
                        "opcion_del_menu",
                        "fecha_larga",
                        "ani",
                        "tmo",
                        "ejecutivo",
                        "respuesta_pregunta_1",
                        "respuesta_pregunta_2",
                        "definicion",
                    ]
                if report_datetime == current_year_month:
                        df = df[check_columns]
                        df["execution_date"] = ds
                        ds_month = date_time.strptime(ds, "%Y-%m-%d").replace(day=1)
                        df["execution_month"] = ds_month.strftime("%Y-%m-%d")
                        table_name = f"temp.call_south_sftp_{self._ds_nodash}"
                        df.to_gbq(table_name, project_id=self._project_source, if_exists="replace")

    def _generate_insert(self):
        self._execute_query(path=INSERT_PATH)

    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)

    def _parse_query(self, query):
        return (
            query.replace(r"{{ds_nodash}}", self._ds_nodash)
            .replace(r"${project_source}", self._project_source)
            .replace(r"${project_target}", self._project_target)
            .format(**{"project_id": self._project_id})
        )
    
    def _remove_file(self):
        """This function remove the file previously download it"""
        files_list = os.listdir()
        for file in files_list:
            if 'Reporte Tenpo IVR Gestiones' in file:
                os.remove(file)
