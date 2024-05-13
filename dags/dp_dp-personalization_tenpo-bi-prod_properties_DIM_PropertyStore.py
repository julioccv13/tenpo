from datetime import datetime
import json
from airflow import models
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

DAG_ID = "dp_dp-personalization_tenpo-bi-prod_properties_DIM_PropertyStore"
SCHEDULE = "0 0 * * *"
PROJECT_ID = "tenpo-datalake-prod"
TOPIC_ID = "topic_data-product-gke-resources"

def message_encode(process_date: str):
    message = {'template_uri': 'gs://bkt-data-product-b728d9b1-prod/data-product/micro-services/data-product-dwh-extractor/job.yaml', 'project_id': 'tenpo-datalake-prod', 'process_date': '<process_date>', 'input_contract_uri': 'gs://bkt-data-product-b728d9b1-prod/data-product/config/tenpo-bi-prod/properties/DIM_PropertyStore/contract.json', 'gke': {'cluster_name': 'prod-k8s-cluster', 'zone': 'us-east4', 'namespace': 'data-product'}}
    message_str = json.dumps(message)
    message_str = message_str.replace("<process_date>", process_date)
    return message_str.encode('utf-8')

def endpoint_encode(process_date: str):
    message = {'template_uri': 'gs://bkt-data-product-b728d9b1-prod/data-product/config/tenpo-bi-prod/properties/DIM_PropertyStore/endpoint/pod.yaml', 'project_id': 'tenpo-datalake-prod', 'process_date': '<process_date>', 'input_contract_uri': 'gs://bkt-data-product-b728d9b1-prod/data-product/config/tenpo-bi-prod/properties/DIM_PropertyStore/contract.json', 'gke': {'cluster_name': 'prod-k8s-cluster', 'zone': 'us-east4', 'namespace': 'data-product'}}
    message_str = json.dumps(message)
    message_str = message_str.replace("<process_date>", process_date)
    return message_str.encode('utf-8')

with models.DAG(
    DAG_ID,
    schedule_interval=SCHEDULE,
    start_date=datetime(2018, 1, 1),
    catchup=False,
    user_defined_macros={
        "pubsub_encode": message_encode,
        "endpoint_encode": endpoint_encode
    },
    render_template_as_native_obj=True,
) as dag:
    
    message = "{{ pubsub_encode(ds) }}"
    endpoint_message = "{{ endpoint_encode(ds) }}"

    pubsub_publisher = PubSubPublishMessageOperator(
        task_id="data_sink",
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
        messages=[
            {"data": message},
        ],
    )

    pubsub_endpoint_publisher = PubSubPublishMessageOperator(
        task_id="endpoint",
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
        messages=[
            {"data": endpoint_message},
        ],
    )


    pubsub_publisher >> pubsub_endpoint_publisher