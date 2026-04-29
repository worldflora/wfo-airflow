
import pendulum
from airflow.sdk import dag, task, Variable, Param
from datetime import timedelta
from airflow.sdk.exceptions import AirflowFailException
import os
import re
import json
from includes.portal_api import PortalApi
from includes.fyllo_api import FylloApi

# needed for macos running
os.environ['NO_PROXY'] = '*'

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "portal"],
    params={"index_all":
        Param( 
            type="boolean",
            title="Reindex all the facets:", 
            default=False,
            description="By default this will only index the facet metadata that has changed since the last index run. Check this box to index them all." )},
)
def portal_index_facets_metadata():
    """
    ### Update the facets metadata in the portal. Only does modified unless prompted otherwise.
    """
    
    @task()
    def setup_local_storage(**context):
        base = Variable.get("local-file-storage")
        subdir = base + "/portal_index_facets_metadata/"
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        safe_file_name = re.sub(r"[/\\?%*:|\"<>\x7F\x00-\x1F]", "-", context["run_id"])
        file_path = subdir + safe_file_name
        print(file_path)
        context["ti"].xcom_push(key="file_path", value=file_path)

    @task()
    def fetch_last_modified_from_portal(**context):

        # if flagged to index all we simple index from the start of the epoc
        if context["params"]["index_all"]:
            context["ti"].xcom_push(key="since", value=0)
            return 0

        # didn't index all so we are getting the 
        print('calling portal')
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        since =  portal.getFacetMetadataLastModified()
        context["ti"].xcom_push(key="since", value=since)
        return since

    @task()
    def fetch_metadata_from_fyllo(**context):

        # get the metadata
        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        since = context["ti"].xcom_pull(task_ids="fetch_last_modified_from_portal", key="since")
        metadata = fyllo.fetchFacetMetadata(since)
        print(f"Metadata documents fetched: {len(metadata['docs'])}")

        # write it to a file
        file_path = context["ti"].xcom_pull(task_ids="setup_local_storage", key="file_path")
        with open(file_path, "w") as f:
            json.dump(metadata, f)
        
    @task()
    def push_metadata_to_portal(**context):

        # read the metadata from the file
        file_path = context["ti"].xcom_pull(task_ids="setup_local_storage", key="file_path")
        with open(file_path, "r") as f:
            metadata = json.load(f)

            # post it to the portal
            portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
            response = portal.pushFacetMetadata(metadata)
            if not response['success']: 
                print(response['message'])
                raise AirflowFailException("Failed to save metadata to index")
    
    @task()
    def tear_down_local_storage(**context):
        file_path = context["ti"].xcom_pull(task_ids="setup_local_storage", key="file_path")
        if os.path.exists(file_path):
            os.remove(file_path)
            
    # dag wiring diagram
    setup_local_storage() >> fetch_last_modified_from_portal() >> fetch_metadata_from_fyllo() >>  push_metadata_to_portal() >> tear_down_local_storage()

portal_index_facets_metadata()


