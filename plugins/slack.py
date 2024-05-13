from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


def get_task_success_slack_alert_callback(slack_conn_id, **others):
    def task_success_slack_alert(context):
        slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
        slack_msg = """
                :red_circle: Task Failed. 
                *Task*: {task}  
                *Dag*: {dag} 
                *Execution Time*: {exec_date}  
                *Log Url*: {log_url} 
                """.format(
                task=context.get('task_instance').task_id,
                dag=context.get('task_instance').dag_id,
                ti=context.get('task_instance'),
                exec_date=context.get('execution_date'),
                log_url=context.get('task_instance').log_url,
            )
        failed_alert = SlackWebhookOperator(
            task_id='slack_notification',
            http_conn_id=slack_conn_id,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username='airflow')
        return failed_alert.execute(context=context)
    return task_success_slack_alert



