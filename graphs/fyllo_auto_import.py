
import pendulum
from airflow.sdk import dag, task, Variable, Param
from airflow.exceptions import AirflowFailException
from airflow.timetables.trigger import DeltaTriggerTimetable
import os
import json
from datetime import timedelta
from includes.portal_api import PortalApi
from includes.fyllo_api import FylloApi

# needed for macos running
os.environ['NO_PROXY'] = '*'

@dag(
    schedule=DeltaTriggerTimetable(timedelta(minutes=30)),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "fyllo"],
)
def fyllo_auto_import():
    """
    ### Import the oldest stale datasource into Fyllo
    """

    @task.short_circuit()
    def fetch_next_import_job(**context):

        # get a handle on the portal
        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        data = fyllo.fetchNextImportJob()

        if data['import'] and data['local_file_path']:
            context["ti"].xcom_push(key="source_data", value=data)
            return True # continue to next task
        else:
            print("No datasources need processing")
            return False # stop the dag now.
    
    @task()
    def page_through_source(**context):

        source_data = context["ti"].xcom_pull(task_ids="fetch_next_import_job", key="source_data")
        
        print(f"Processing datasource: {source_data['source_id']}")

        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        source_data['page_size'] = 100 # small page size to prevent timeouts 

        while source_data := fyllo.pageImportJob(source_data):
            
            # get out of here if we have finished
            if source_data['finished']: break
            
            source_data['offset'] = int(source_data['offset']) + int(source_data['page_size'])
            
            source_data['processed_rows'] = 0 # we go around again

            # prevent infinite looping
            if int(source_data['offset']) > 1000000:
                raise AirflowFailException("Over a million names in the datasource. Something is wrong.")

            print(source_data)
            
        # print the last one we produce
        print(source_data)

    # dag wiring diagram
    fetch_next_import_job() >> page_through_source()

fyllo_auto_import()


