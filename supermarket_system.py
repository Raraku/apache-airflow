from airflow.sensors.filesystem import FileSensor
from pathlib import Path
from airflow.sensors.python import PythonSensor
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import airflow.utils.dates as dates

dag = DAG(
    dag_id="supermarket_data_pipeline",
    start_date=dates.days_ago(0),
    # schedule_interval="@hourly",
    schedule_interval=None,
)

# wait_for_supermarket_1 = FileSensor(
#     task_id="wait_for_supermarket_1", filepath="/data/supermarket1/data.csv"
# )

# More nuanced sensor
def _wait_for_supermarket(supermarket_id):
    supermarket_path = Path("/data" + supermarket_id)
    data_files = supermarket_path.glob("data-*.csv")
    success_file = supermarket_path / "_SUCCESS"
    return data_files and success_file.exists()


wait_for_supermarket_1 = PythonSensor(
    task_id="wait_for_supermarket_1",
    python_callable=_wait_for_supermarket,
    op_kwargs={"supermarket_id": "supermarket1"},
    dag=dag,
)
