import re
import os
import datetime
import pendulum 
from airflow.sdk import dag, task, Variable, Param
from airflow.exceptions import AirflowFailException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLThresholdCheckOperator

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "database"],
    params={"fail_if_db_not_todays":
             Param(True, 
                   type="boolean", 
                   title="Fail if not today's db:", 
                   description="If the latest db dump is not dated today then this will fail. " \
                   "Set to false to import a dump from a previous day e.g. if there was a fail." )},
    render_template_as_native_obj=True
)
def import_to_staging():
    """
    ### Restore db

    The live server uses rsync to copy across backup files every day.
    This will import the most recent databased dump file
    (in the wfo-rhakhis-backups-dir) into the local instance of MySQL.

    When run on a schedule it will fail if the db dump does
    not have the file name of today. 

    When triggered directly then an older file can be loaded.

    Note that this uses BOTH the MySQL connection set up in Airflow 
    and also requires credentials in .my.cnf so that it can pipe
    the main data gzip file through mysql on the command line.
    
    """
    @task()
    def check_backup_file_exists(**context):
        """
        Checks the directory exist and there is a file of 
        reasonable size with the correct date
        """
        backup_dir = Variable.get("wfo-rhakhis-backups-dir")
        if not backup_dir.endswith('/'):
            backup_dir = backup_dir + '/'

        # freak if we don't have a backups db
        if os.path.isdir(backup_dir):
            print(backup_dir)
        else:
            raise AirflowFailException(f"Backups directory is not found {backup_dir}")
        

        # Check we have a bunch of files that meet the regex for download
        file_name_regex = "^promethius_[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}.sql.gz$"
        files = [f for f in os.listdir(backup_dir) if os.path.isfile(backup_dir + f) and re.search(file_name_regex, f) ]
        print(files)
        if len(files) < 1:
            raise AirflowFailException(f"No backups files matching {file_name_regex} found in directory {backup_dir}")
        files.sort()
            
        # get the latest one
        file_name = files[-1]

        # fail if it isn't today and we haven't set an overide 
        today_string = datetime.datetime.now().strftime("%Y-%m-%d")
        if not re.search("^promethius_" + today_string + "-[0-9]{2}-[0-9]{2}-[0-9]{2}.sql.gz$", file_name) and context["params"]["fail_if_db_not_todays"]:
            raise AirflowFailException(f"No backups files matching today's date ({today_string})found in directory {backup_dir}")

        # we are good to go with the actual db import
        return backup_dir + file_name

    drop_old_db = SQLExecuteQueryOperator(
        task_id="drop_old_db",
        conn_id="airflow_wfo",
        sql=f"DROP DATABASE IF EXISTS promethius;",
        return_last=False,
    )

    create_new_db = SQLExecuteQueryOperator(
        task_id="create_new_db",
        conn_id="airflow_wfo",
        sql=f"CREATE DATABASE promethius;",
        return_last=False,
    )

    @task.bash()
    def mysql_import(**context):
        gzip_path = context["ti"].xcom_pull(task_ids="check_backup_file_exists", key="return_value")
        # add the zip path to the context so we can use it in the import task
        sql_path = os.path.splitext(gzip_path)[0]
        print(sql_path)
        context["ti"].xcom_push(key="sql_path", value=sql_path)

        # Pipe it into mysql
        return f"gunzip < \"{gzip_path}\" | mysql promethius"
        #return 'ls'
    
    sanity_check_name_count = SQLThresholdCheckOperator(
        task_id="sanity_check_name_count",
        conn_id="airflow_wfo",
        sql="SELECT count(*) FROM `promethius`.`names`;",
        min_threshold=1600000,
        max_threshold=2000000
    )

    check_backup_file_exists() >> drop_old_db >> create_new_db >> mysql_import() >> sanity_check_name_count
    
import_to_staging()