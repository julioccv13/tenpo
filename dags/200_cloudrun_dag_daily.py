from datetime import timedelta, datetime
from typing import Dict
import pendulum
import requests
from time import sleep

import airflow
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.latest_only_operator import LatestOnlyOperator
from google.cloud import firestore
from threading import Thread

environment = Variable.get("environment")
dbt_cloudrun_url = Variable.get("dbt_cloudrun_url")
utcnow = datetime.utcnow().strftime("%Y%m%d%H")
SENSIBLE_SHA256_PEPPER_SECRET = Variable.get(
    "sensible_sha256_pepper_secret", deserialize_json=False)
dict_projects = {f'project{i}': Variable.get(f'project{i}') for i in range(1, 29)}

default_args = {
    'owner': 'Jarvis',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 6, 1, tz="UTC"),
    'email': ['jarvis@tenpo.cl'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1)
}


def get_firestore(utcnow):
    project = 'tenpo-datalake-sandbox'
    db = firestore.Client(project)
    doc_ref = db.collection(u'DBT').document(utcnow)

    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict()
    else:
        raise RuntimeError('No such document on Firestore!')


def factory(script):
    @task(task_id=script.replace(".sh", ""))
    def temp():

        class MyThread(Thread):
            json = {
                "dbt_files_uri": f"gs://{environment}-source-files/200/200.zip",
                "dbt_script": script,
                "target": "prod",
                "sensible_sha256_pepper_secret": SENSIBLE_SHA256_PEPPER_SECRET,
                "dict_projects": dict_projects
            }
            dbt_cloudrun_url = dbt_cloudrun_url

            def run(self):
                self._return = requests.post(self.dbt_cloudrun_url, json=self.json)

            def join(self, *args):
                Thread.join(self)
                return self._return

        response = MyThread()
        response.start()
        response_catched = response.join()

        while True:
            sleep(60)
            data = get_firestore(utcnow)
            status = data['status']
            if status == 'RUNNING':
                continue
            elif status == 'FAILED':
                raise RuntimeError(data['traceback'])
            elif status == 'SUCCEEDED':
                return response_catched.text, response_catched.status_code
    return temp


with airflow.DAG(
    '200_DBT_Tenpo_BI_Daily',
    catchup=False,
    default_args=default_args,
    schedule_interval="30 10 * * *"
) as dag:

    is_this_latest_dag_run = LatestOnlyOperator(task_id="is_latest_dagrun", dag=dag)

    tasks = [is_this_latest_dag_run]
    scripts = ['00a_DBT_FRESHNESS.sh', '00b_DBT_SEED.sh', '01_DBT_RUN.sh',
               '02_DBT_TEST.sh', '03_DBT_SNAPSHOT.sh', '04_DBT_DOCS.sh']
    for script in scripts:
        tasks.append(factory(script)())
        tasks[-2] >> tasks[-1]
