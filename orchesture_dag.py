import datetime
from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
from kubernetes.client import models as k8s_models
from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
import sys
import logging
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import json
LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
LOGGING_CONFIG["handlers"]["stdout"] = {
    "class": "logging.StreamHandler",
    "formatter": "airflow",
    "stream": sys.stdout,
}
IMAGE = "australia-southeast1-docker.pkg.dev/uowaikato-main/meltano/meltano-dv360-test:prod"
LOGGING_CONFIG["loggers"]["airflow.task"]["handlers"] = ["stdout", "task"]
log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)
local_tz = pendulum.timezone("Pacific/Auckland")
start_date = datetime.datetime(2024, 1, 1, tzinfo=local_tz)

yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=1)
default_args = {
    "retries": 2,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": yesterday
}

def get_secret():
    """
    Fetches a secret's value from Google Secret Manager and returns it as a dictionary.
    """
    from google.cloud import secretmanager
    import json

    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/739679429225/secrets/airflow-variables-meltano_uowaikato_main/versions/latest"
    response = client.access_secret_version(name=secret_name)
    secret_payload = response.payload.data.decode("UTF-8")
    return json.loads(secret_payload)  # Return as a parsed dictionary


meltano_env = get_secret()
start_date_str = start_date.strftime("%Y-%m-%d")
meltano_env["START_DATE"] = start_date_str
meltano_env["BQ_METHOD"] = "batch_job"

def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_copy = meltano_env.copy()
    return meltano_env_copy
with models.DAG(
    dag_id="uowaikato-test-dv360",
    schedule_interval="0 6 * * *",
    default_args=default_args,
) as dag:
    def set_env_vars(**context):
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360"
        env["BQ_METHOD"] = "batch_job"
        return env
    name = "dv360-to-bigquery"
    kube_tiktok = KubernetesPodOperator(
        name=name,
        task_id="dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "dv360-121", "target-bigquery"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "4000M", "cpu": "1000m"},
        ),
        base_container_name=f"meltano-{name}",
    )