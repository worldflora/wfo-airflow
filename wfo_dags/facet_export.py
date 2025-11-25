
import pendulum
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import sys
from includes.export import process_data

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "util", "facet"],
)
def facet_export():
    """
    ### Export data sources to a range of format files.
    """
    @task.bash()
    def bash_list():
        """
        #### Bash ls
        Just lists the current directory contents.
        """
        return "ls"

    show_tables = SQLExecuteQueryOperator(
        task_id="show_tables",
        conn_id="airflow_wfo",
        sql="SHOW TABLES FROM `promethius`;"
    )

    @task()
    def print_tables(**context):
        print(sys.path)
        sql_results = context["ti"].xcom_pull(task_ids="show_tables", key="return_value")
        print(sql_results)

    bash_list() >> show_tables >> print_tables()

facet_export()
