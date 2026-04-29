
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
    def fetch_last_modified_from_portal(**context):

        # if flagged to index all we simple index from the start of the epoc
        if context["params"]["index_all"]: return 0

        # didn't get index all so we are getting the 
        print('calling portal')
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        since =  portal.getFacetMetadataLastModified()
        return since

    @task()
    def fetch_and_push(since):

        # fetch it from fyllo
        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        metadata = fyllo.fetchFacetMetadata(since)
        print(f"Metadata documents fetched: {len(metadata['docs'])}")

        # post it to the portal
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        response = portal.pushFacetMetadata(metadata)
        if not response['success']: 
            print(response['message'])
            raise AirflowFailException("Failed to save metadata to index")
            
    # dag wiring diagram
    fetch_and_push(fetch_last_modified_from_portal())

portal_index_facets_metadata()


