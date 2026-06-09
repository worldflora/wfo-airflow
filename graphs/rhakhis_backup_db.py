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
    schedule="12 0 * * *", # 00:12 each day we back the db up
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), # arbitrary
    catchup=False,
    max_active_runs=1,
    tags=["wfo", "rhakhis", "solstice", "util"],
)
def rhakhis_backup_db():
    """ 
        This will backup the live Rhakhis database 
        It will only do it if the backup file for the day doesn't
        already exist.
    """
    
    @task.short_circuit()
    def check_backup_file_does_not_exist(**context):
        """
        Checks the directory exist and it doesn't already contain a file for the day.
        """
        backup_dir = Variable.get("wfo-rhakhis-backups-dir")
        if not backup_dir.endswith('/'):
            backup_dir = backup_dir + '/'

        print(os.listdir(backup_dir))

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

        # fail if there is a backup for today already
        today_string = datetime.now().strftime("%Y-%m-%d")
        if re.search("^promethius_" + today_string + "-[0-9]{2}-[0-9]{2}-[0-9]{2}.sql.gz$", file_name):
            print(f"A file matching today's date ({file_name}) was found in the directory so will not continue.")
            return False
        else:
            # file for today doesn't exist so we can continue to generate the backup
            return True

    @task()
    def run_mysql_backup(**context):
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


    check_backup_file_does_not_exist() >> run_mysql_backup() >> rotate_backup_files()

rhakhis_backup_db()


