from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Définir les arguments par défaut
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 3),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG
with DAG(
    'audio_segmentation_pipeline',
    default_args=default_args,
    description='Pipeline de segmentation audio avec Kafka et PostgreSQL',
    schedule_interval=None,  
    catchup=False,
) as dag:

    # Fonction pour démarrer le producer
    def run_producer():
        producer_script = "/opt/airflow/scripts/producer.py"
        subprocess.run(['python', producer_script])

    # Fonction pour démarrer le consumer
    def run_consumer():
        consumer_script = "/opt/airflow/scripts/consumer.py"
        subprocess.run(['python', consumer_script])

    # Tâche pour lancer le producer
    start_producer = PythonOperator(
        task_id='start_producer',
        python_callable=run_producer,
        dag=dag,
    )

    # Tâche pour lancer le consumer
    start_consumer = PythonOperator(
        task_id='start_consumer',
        python_callable=run_consumer,
        dag=dag,
    )

    # Exécution parallèle des tâches
    start_producer >> start_consumer  # Le consumer attend que le producer soit terminé
