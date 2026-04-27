
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
            title="Reindex all the snippets BEWARE!", 
            default=False,
            description="By default this will only index the snippets metadata that has changed since the last run. This will delete snippets in index and rebuild them one thousand at a time, which might take a while." )},
)
def portal_index_snippets_metadata():
    """
    ### Update the facets metadata in the portal. Only does modified unless prompted otherwise.
    """

    @task()
    def delete_snippets_metadata(**context):
        # if flagged to index all we simple index from the start of the epoc
        if context["params"]["index_all"]:
            print("Deleting all snippets from index")
            portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
            response = portal.deleteSnippetsMetadata()
            if response['success']: return True
            else:
                print(response['message'])
                print(response['solr_response'])
                raise AirflowFailException("Failed to delete metadata documents")
        else:
            print("Updating changed snippets")
            return False
    
    @task()
    def fetch_last_modified_from_portal(deleted):
        print('calling portal')
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        return portal.getSnippetsMetadataLastModified()

    @task()
    def fetch_metadata_from_fyllo(since):
        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        metadata = fyllo.fetchSnippetsMetadata(since)
        print(f"Metadata documents fetched: {len(metadata['docs'])}")
        return metadata
    
    @task()
    def push_metadata_to_portal(metadata):
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        if len(metadata['docs']) > 0:
            response = portal.pushSnippetsMetadata(metadata)
            if not response['success']: 
                print(response['message'])
                raise AirflowFailException("Failed to save metadata to index")
        else:
            print("Nothing to push")
        
    # dag wiring diagram
    push_metadata_to_portal(fetch_metadata_from_fyllo(fetch_last_modified_from_portal(delete_snippets_metadata())))

portal_index_snippets_metadata()


