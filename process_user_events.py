import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="01_unscheduled",
    start_date=dt.datetime(2019, 1, 1),
    schedule_interval=None,
    # schedule_interval = dt.timedelta(days=3)
    end_date=dt.datetime(year=2019, month=1, day=5),
)

# fetch_events = BashOperator(
#     task_id="fetch_events",
#     bash_command=(
#         "mkdir -p /data &&" "curl -o /data/events.json" "https://localhost:5000/events"
#     ),
#     dag=dag,
# )

# v2
# fetch_events = BashOperator(
#     task_id="fetch_events",
#     bash_command=(
#         "mkdir -p /data && "
#         "curl -o /data/events.json "
#         "http://localhost:5000/events?"
#         "start_date={{execution_date.strftime('%Y-%m-%d')}}"
#         "&end_date={{next_execution_date.strftime('%Y-%m-%d')}}"
#     ),
#     dag=dag,
# )

# v3
# fetch_events = BashOperator(
#     task_id="fetch_events",
#     bash_command=(
#         "mkdir -p /data && "
#         "curl -o /data/events.json "
#         "http://localhost:5000/events?"
#         "start_date={{ds}}&"
#         "end_date={{next_ds}}"
#     ),
#     dag=dag,
# )

# v4 - partitioning
fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data && "
        "curl -o /data/events/{{ds}}.json "
        "http://localhost:5000/events?"
        "start_date={{ds}}&"
        "end_date={{next_ds}}"
    ),
    dag=dag,
)

# v1
# def _calculate_stats(input_path, output_path):
#     """Calculate Event Statistics"""
#     events = pd.read_json(input_path)
#     stats = events.groupby(["date", "user"]).size().reset_index()
#     Path(output_path).parent.mkdir(exist_ok=True)
#     stats.to_csv(output_path, index=False)

# v2 - templating
def _calculate_stats(**context):
    """Calculate Event Statistics"""
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


# v1
# calculate_stats = PythonOperator(
#     task_id="calculate_stats",
#     python_callable=_calculate_stats,
#     op_kwargs={"input_path": "/data/events.json", "output_path": "/data/stats.csv"},
#     dag=dag,
# )

# v2
calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    templates_dict={
        "input_path": "/data/events/{{ds}}.json",
        "output_path": "/data/stats/{{ds}}.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats
