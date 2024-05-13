from datetime import datetime
import json
from airflow import models
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

DAG_ID = "credit-risk-financial-reporting"
SCHEDULE = "20 */1 * * *"
PROJECT_ID = "tenpo-datalake-prod"
TOPIC_ID = "topic_data-product-gke-resources"

def push_method(process_date: str):
    message = {
        'template_uri': 'gs://bkt-data-product-b728d9b1-prod/credit-risk/micro-services/financial-reporting/credit-risk-push.yaml',
        'project_id': 'tenpo-datalake-prod', 'process_date': '<process_date>',
        'gke': {
            'cluster_name': 'prod-k8s-cluster', 
            'zone': 'us-east4', 
            'namespace': 'streaming'
            }
        }
    message_str = json.dumps(message)
    message_str = message_str.replace("<process_date>", process_date)
    return message_str.encode('utf-8')
    message = {
        'template_uri': 'gs://bkt-data-product-b728d9b1-prod/credit-risk/micro-services/financial-reporting/credit-risk-pull.yaml',
        'project_id': 'tenpo-datalake-prod',
        'process_date': '<process_date>',
        'gke': {
            'cluster_name': 'prod-k8s-cluster',
            'zone': 'us-east4',
            'namespace': 'streaming'
            }
        }
    message_str = json.dumps(message)
    message_str = message_str.replace("<process_date>", process_date)
    return message_str.encode('utf-8')

with models.DAG(
    DAG_ID,
    schedule_interval=SCHEDULE,
    start_date=datetime(2018, 1, 1),
    catchup=False,
    user_defined_macros={
        "pubsub_encode": push_method
    },
    render_template_as_native_obj=True,
) as dag:
    
    message = "{{ pubsub_encode(ds) }}"

    pubsub_publisher = PubSubPublishMessageOperator(
        task_id="push",
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
        messages=[
            {"data": message},
        ],
    )



    pubsub_publisher