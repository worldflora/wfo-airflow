
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
    schedule=DeltaTriggerTimetable(timedelta(seconds=30)),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1, # important because these may overlap if run at 30 second intervals.
    tags=["wfo", "portal"]
)
def portal_index_page_of_taxa():
    """
    ### Run through all taxa and index them, 1,000 per page.
    """
    @task()
    def page_through_taxa(**context):

        page_size = 1000

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

    # dag wiring diagram
    page_through_taxa()

portal_index_page_of_taxa()


