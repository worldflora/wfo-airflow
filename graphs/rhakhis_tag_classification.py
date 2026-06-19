import os
import time
import re
import pendulum
from datetime import datetime
import glob
import zipfile
from importlib import resources
import subprocess
from airflow.sdk import dag, task, Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowFailException

os.environ['NO_PROXY'] = '*' # needed to run on mac in dev environment.

@dag(
    schedule=None, # 00:12 each day we back the db up
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), # arbitrary
    catchup=False,
    max_active_runs=1,
    tags=["wfo", "rhakhis", "solstice", "util"],
)
def rhakhis_tag_classification():
    """ 
        This will update the previous placements table with the
        difference between the last classification in that table and the
        current placements of names.
    """
    
    @task.short_circuit()
    def check_classification_not_tagged(**context):
        """
        We should only run if this classification has not been tagged
        before.
        """
        classification_name = datetime.now().strftime("%Y-%m")
        
        # get the db connection
        mysql_hook = MySqlHook(mysql_conn_id="rhakhis_db")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""SELECT i.`value` FROM `promethius`.`names` as n JOIN `promethius`.identifiers as i on n.prescribed_id = i.id and i.kind = 'wfo'
                WHERE n.`status` != 'deprecated' ORDER BY n.name_alpha LIMIT %s OFFSET %s""", (page_size, offset))
        rows = cursor.fetchall()


    @task()
    def run_tagging(**context):
        """
        Run MySQL Dump command
        """

        backup_dir = Variable.get("wfo-rhakhis-backups-dir")
        if not backup_dir.endswith('/'):
            backup_dir = backup_dir + '/'
        database_name = Variable.get("wfo-rhakhis-database-name")
    
        file_path = backup_dir + database_name + "_"  + datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + '.sql.gz'

        command = f"mysqldump {database_name} | gzip > {file_path}"

        print(command)

        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode:
            print(result.stdout)
            raise AirflowFailException(f"Failed to create {file_path} from database {database_name}")
        else:
            print(file_path)
            print('Created OK')

    @task()
    def rotate_backup_files():
        """
        Only keep backups made in the last 30 days
        """
        backup_dir = Variable.get("wfo-rhakhis-backups-dir")

        command = f"find {backup_dir} -type f -mtime +30 -delete"
        print(command)
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode:
            print(result.stdout)
            raise AirflowFailException(f"Failed to delete excess files in {backup_dir}")
        else:
            print(backup_dir)
            print('Deletes OK')


    check_classification_not_tagged() >> run_mysql_backup() >> rotate_backup_files()

rhakhis_backup_db()


