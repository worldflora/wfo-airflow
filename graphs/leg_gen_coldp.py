import os
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
    schedule="30 0 * * *", # 18 minutes after backup scheduled
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), # arbitrary
    catchup=False,
    max_active_runs=1,
    tags=["wfo", "rhakhis", "solstice", "util", "legacy"],
)
def leg_gen_coldp():
    """ 
    Animate generation of Catalogue of Life Data Package.
    Useful during prep for data publication. May not run year around.
    """
    @task()
    def run_php_generation_script():
        """
        We are just a wrapper around the existing PHP generation script
        """
        scripts_dir = Variable.get("wfo-rhakhis-api-scripts-dir")

        date_string = datetime.now().strftime("%Y-%m-%d")

        command = f"cd {scripts_dir}; php -d memory_limit=4G gen_coldp.php {date_string} 01"
        print(command)
        result = subprocess.run(command, shell=True, capture_output=False, text=True)
        if result.returncode:
            print(result.stdout)
            raise AirflowFailException(f"Failed to generate CoLDP file.")
        else:
            print('Generated OK')


    run_php_generation_script()

leg_gen_coldp()


