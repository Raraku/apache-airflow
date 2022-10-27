import uuid
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow import DAG
import airflow.utils.dates as dates
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.dummy import DummyOperator
import pendulum
from airflow.decorators import task

# start = DummyOperator(task_id="start")

dag = DAG(dag_id="weather_ai", start_date=dates.days_ago(0), schedule_interval=None)


start = DummyOperator(task_id="start")


def _fetch_weather():
    print("weather")


def _clean_weather():
    print("cleaning weather")


def _fetch_sales_old():
    print("sales")


def _clean_sales_old():
    print("cleaning sales")


def _fetch_sales_new():
    print("sales")


def _clean_sales_new():
    print("cleaning sales")


def _join_datasets():
    print("join datasets")


# def _train_model(**context):
#     model_id = str(uuid.uuid4())
#     context["task_instance"].xcom_push(key="model_id", value=model_id)

#     print("training model")


def _deploy_model(**context):
    model_id = context["task_instance"].xcom_pull(
        task_ids="train_model", key="model_id"
    )
    print(f"Deploying model {model_id}")
    # print("deploying model")


# def _deploy_model(templates_dict, **context):
#             model_id = templates_dict["model_id"]
#             print(f"Deploying model {model_id}")
#         deploy_model = PythonOperator(
#             task_id="deploy_model",
#             python_callable=_deploy_model,
#             templates_dict={
# "model_id": "{{task_instance.xcom_pull(
# ➥ task_ids='train_model', key='model_id')}}"
# }, )

ERP_SWITCH_DATE = dates.days_ago(2)


def _pick_erp_system(**context):
    if context["execution_date"] < ERP_SWITCH_DATE:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"


def _latest_only(**context):
    left_window = context["dag"].following_schedule["execution_date"]
    right_window = context["dag"].following_schedule[left_window]
    now = pendulum.now("UTC")
    if not left_window < now <= right_window:
        raise AirflowSkipException("Not the most recent run!")


pick_erp_system = BranchPythonOperator(
    task_id="pick_erp_system",
    python_callable=_pick_erp_system,
)

fetch_weather = PythonOperator(task_id="fetch_weather", python_callable=_fetch_weather)
clean_weather = PythonOperator(task_id="clean_weather", python_callable=_clean_weather)
# fetch_sales = PythonOperator(task_id="fetch_sales", python_callable=_fetch_sales)
# clean_sales = PythonOperator(task_id="clean_sales", python_callable=_clean_sales)

fetch_sales_old = PythonOperator(
    task_id="fetch_sales_old", python_callable=_fetch_sales_old
)
clean_sales_old = PythonOperator(
    task_id="clean_sales_old", python_callable=_clean_sales_old
)
fetch_sales_new = PythonOperator(
    task_id="fetch_sales_new", python_callable=_fetch_sales_new
)
clean_sales_new = PythonOperator(
    task_id="clean_sales_new", python_callable=_clean_sales_new
)
join_branch = DummyOperator(task_id="join_erp_branch", trigger_rule="none_failed")

join_datasets = PythonOperator(task_id="join_datasets", python_callable=_join_datasets)
# train_model = PythonOperator(task_id="train_model", python_callable=_train_model)
# latest_only = PythonOperator(
#     task_id="latest_only",
#     python_callable=_latest_only,
#     dag=dag,
# )
latest_only = LatestOnlyOperator(
    task_id="latest_only",
    dag=dag,
)
# deploy_model = PythonOperator(task_id="deploy_model", python_callable=_deploy_model)


@task
def train_model():
    model_id = str(uuid.uuid4())
    return model_id


@task
def deploy_model(model_id):
    print(f"Deploying model {model_id}")


model_id = train_model()
deploy_model(model_id)

start >> [fetch_weather, pick_erp_system]
pick_erp_system >> [fetch_sales_old, fetch_sales_new]
fetch_sales_old >> clean_sales_old
fetch_sales_new >> clean_sales_new
fetch_weather >> clean_weather
[clean_sales_old, clean_sales_new] >> join_branch
[clean_weather, join_branch] >> join_datasets
join_datasets >> train_model >> latest_only >> deploy_model
