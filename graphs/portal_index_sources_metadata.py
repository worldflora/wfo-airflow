
import pendulum
from airflow.sdk import dag, task, Variable, Param
from datetime import timedelta
from airflow.timetables.trigger import DeltaTriggerTimetable
from airflow.sdk.exceptions import AirflowFailException
import os
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
            title="Reindex all the sources:", 
            default=False,
            description="By default this will only index the facet and snippet sources that have changed since the last index run. Check this box to index them all." )},
)
def portal_index_sources_metadata():
    """
    ### Update the sources metadata in the portal. Only does modified unless prompted otherwise
    """
    
    @task()
    def fetch_last_modified_from_portal(**context):

        # if flagged to index all we simple index from the start of the epoc
        if context["params"]["index_all"]: return 0

        # didn't index all so we are getting the 
        print('calling portal')
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        return portal.getSourceMetadataLastModified()

    @task()
    def fetch_metadata_from_fyllo(since):
        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        metadata = fyllo.fetchSourceMetadata(since)
        print(f"Metadata documents fetched: {len(metadata['docs'])}")
        return metadata
    
    @task()
    def push_metadata_to_portal(metadata):
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        response = portal.pushSourceMetadata(metadata)
        if not response['success']: 
            print(response['message'])
            raise AirflowFailException("Failed to save metadata to index")
            
    # dag wiring diagram
    push_metadata_to_portal(fetch_metadata_from_fyllo(fetch_last_modified_from_portal()))

portal_index_sources_metadata()


