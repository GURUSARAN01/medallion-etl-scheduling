"""Factory for the Azure Silver Static duration pilot."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

DATABRICKS_CONN_ID = "databricks_azure"
TASK_IDS = ("clean_sales", "clean_customer", "clean_item", "join_fact_sales")
STATIC_PRIORITIES = {"clean_sales": 400, "clean_customer": 300, "clean_item": 200, "join_fact_sales": 100}

def build_azure_silver_static_pilot_dag():
    cluster_id = Variable.get("databricks_azure_cluster_id", default_var="SET_DATABRICKS_CLUSTER_ID")
    notebook_path = Variable.get("databricks_silver_notebook_path", default_var="/Shared/05_silver_task")
    target_catalog = Variable.get("databricks_target_catalog", default_var="dbw_thesis_etl_scheduling")
    target_schema = Variable.get("databricks_target_schema", default_var="thesis_etl_scheduling")
    dag = DAG(dag_id="silver_azure_static_pilot", description="Azure Silver TPC-DS SF1 Static duration pilot",
              start_date=datetime(2026,7,1), schedule=None, catchup=False, max_active_tasks=2, max_active_runs=1,
              default_args={"owner":"gurusaran","depends_on_past":False,"retries":0,"execution_timeout":timedelta(minutes=30)},
              tags=["azure","silver","static","pilot","tpcds-sf1"])
    with dag:
        tasks = {}
        for task_id in TASK_IDS:
            priority = STATIC_PRIORITIES[task_id]
            tasks[task_id] = DatabricksSubmitRunOperator(
                task_id=task_id, databricks_conn_id=DATABRICKS_CONN_ID, existing_cluster_id=cluster_id,
                run_name="silver-static-pilot-{{ dag_run.run_id if dag_run else run_id }}-" + task_id,
                notebook_task={"notebook_path":notebook_path,"source":"WORKSPACE","base_parameters":{
                    "task_id":task_id,"strategy":"static","run_id":"{{ dag_run.run_id if dag_run else run_id }}",
                    "priority_weight":str(priority),"target_catalog":target_catalog,"target_schema":target_schema}},
                polling_period_seconds=10, wait_for_termination=True, do_xcom_push=True,
                priority_weight=priority, weight_rule="absolute")
        [tasks["clean_sales"], tasks["clean_customer"], tasks["clean_item"]] >> tasks["join_fact_sales"]
    return dag
