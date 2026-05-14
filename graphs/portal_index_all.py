
import pendulum
from airflow.sdk import dag, task, Variable, Param
from datetime import timedelta
from airflow.sdk.exceptions import AirflowFailException
from airflow.timetables.trigger import DeltaTriggerTimetable
import os
import re
import json
from includes.portal_api import PortalApi
from includes.fyllo_api import FylloApi

# needed for macos running
os.environ['NO_PROXY'] = '*'

doc_md = """\
# Portal Index All

This DAG is designed to run continuously and keep the target portal index up to date with the content of Fyllo.
It uses the natural dependencies in between the different index documents to prioritise the indexing tasks
and make sure the target SOLR index isn't swamped by multiple commits from different DAGs running simultaneously.

Each kind of documents must be entirely up to date before the indexing of the next kind of documents starts
so that we don't get broken links (or at least missing data) within the portal.

1. *Facets* (and their values) are indexed first because they refer to nothing else.
2. *Data Sources* are indexed next because they reference facet values (one per data source).
3. *Scores* are then indexed because they join taxa (that will already be in the index as part of the classification) with data sources and facet values.
4. Text *Snippets* follow. They reference data sources only.
5. *Taxa* are the final things to be indexed as they reference everything!

"""

@dag(
    schedule=DeltaTriggerTimetable(timedelta(minutes=1)),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    doc_md=doc_md,
    tags=["wfo", "portal"],
    params={},
)
def portal_index_all():


    @task.short_circuit()
    def facets(**context):

        print('calling portal for last modified date')
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        since =  portal.getFacetMetadataLastModified()

        # fetch it from fyllo
        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        metadata = fyllo.fetchFacetMetadata(since)
        print(f"Metadata documents fetched: {len(metadata['docs'])}")

        if len(metadata['docs']) > 0:
            # post it to the portal
            response = portal.pushFacetMetadata(metadata)
            if not response['success']: 
                print(response['message'])
                raise AirflowFailException("Failed to save metadata to index")
            print("We haven't finished yet so not going to next document time")
            return False # do not do following steps as we haven't finished indexing all the facets
        else: 
            print("Done all the facets so move to next document kind")
            return True # we've indexed all the facets so move to the next task
    
    @task.short_circuit()
    def sources(**context):
    
        print('calling portal for last modified date')
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        since = portal.getSourceMetadataLastModified()
    
        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        metadata = fyllo.fetchSourceMetadata(since)
        print(f"Metadata documents fetched: {len(metadata['docs'])}")
        
        if len(metadata['docs']) > 0: 
            response = portal.pushSourceMetadata(metadata)
            if not response['success']: 
                print(response['message'])
                raise AirflowFailException("Failed to save metadata to index")            
            print("do not do following steps as we haven't finished indexing all the sources")            
            return False
        else: 
            print("we've indexed all the sources so move to the next task")
            return True
    
    @task.short_circuit()
    def scores(**context):

        print('calling portal for last mod date')
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        since = portal.getScoresMetadataLastModified()

        # from fyllo
        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        metadata = fyllo.fetchScoresMetadata(since)
        print(f"Metadata documents fetched: {len(metadata['docs'])}")

        # to portal
        if len(metadata['docs']) > 0:
            response = portal.pushScoresMetadata(metadata)
            if not response['success']: 
                print(response['message'])
                raise AirflowFailException("Failed to save metadata to index")
            print("we've not finished with scores so don't start on next doc kind")
            return False
        else:
            print("Nothing to push good to go to next task")
            return True # good to continue to next document kind

    @task.short_circuit()
    def snippets(**context):
        
        print('calling portal for last mod')
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        since = portal.getSnippetsMetadataLastModified()
    
        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        metadata = fyllo.fetchSnippetsMetadata(since)
        print(f"Metadata documents fetched: {len(metadata['docs'])}")

        if len(metadata['docs']) > 0:
            response = portal.pushSnippetsMetadata(metadata)
            if not response['success']: 
                print(response['message'])
                raise AirflowFailException("Failed to save metadata to index")
            print("Not finished yet so don't proceed to taxa yet")
            return False
        else:
            print("Nothing to push so good to go to taxa")
            return True
        
    @task
    def taxa():
        
        page_size = 500

        # get a handles on the two applications
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))

        # get the next page of taxon graphs
        taxon_graphs = portal.getTaxonGraphPage(page_size)

        # fail if we didn't get anything
        if not taxon_graphs: raise AirflowFailException(f"No data returned by portal when calling for taxon graphs page size: {page_size}")
        
        # report the page we are on
        taxon_count = len(taxon_graphs)
        print(f"Retrieved {taxon_count} taxa for page size of {page_size}")

        # call fyllo for the values
        values = fyllo.fetchTaxonValues(taxon_graphs)

        # give up if we get nothing back
        if not values : raise AirflowFailException("No data returned by Fyllo")

        # post it back to the portal
        response = portal.pushTaxonValues(values)
        if not response: raise AirflowFailException("No data returned by portal")

        print("Done a page of taxa")

    # dag wiring diagram
    facets() >> sources() >> scores() >> snippets() >> taxa()

portal_index_all()


