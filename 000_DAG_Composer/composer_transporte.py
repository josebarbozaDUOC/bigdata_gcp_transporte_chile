import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

default_args = {
    'owner': 'JOSE MANUEL BARBOZA SEGOVIA',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'composer_transporte',
    default_args=default_args,
    description='Un DAG para ejecutar scripts de Dataflow activado por Pub/Sub',
    schedule_interval=None,  # Sin cronograma interno
)

# Definir el sensor de Pub/Sub
pubsub_sensor = PubSubPullSensor(
    task_id='Programa_Scheduler_Diario',
    project_id='transporte001',
    subscription='projects/transporte001/subscriptions/Iniciar_Composer_Diario_Suscripcion',
    ack_messages=True,
    dag=dag,
)

# Definir la primera tarea usando DataflowCreatePythonJobOperator
task_1_ingesta_historicos = DataflowCreatePythonJobOperator(
    task_id='task_1_ingesta_historicos',
    py_file='gs://transporte001/dataflow_ingesta_historicos.py', 
    job_name='dataflow_ingesta_historicos_job',
    options={
        'project': 'transporte001',
        'region': 'southamerica-west1',
        'input_url': 'https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et',
        'output_bucket': 'test_dataflowcomposer_historicos'
    },
    location='southamerica-west1',
    dag=dag,
)

pubsub_sensor >> task_1_ingesta_historicos