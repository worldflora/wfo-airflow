
import pendulum
from airflow.sdk import dag, task, Variable, Param
from airflow.exceptions import AirflowFailException
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
    params={"wfo_taxon_id":
        Param( 
            type="string",
            title="WFO Taxon ID:", 
            default="wfo-0000072855",
            description="Provide the WFO ID of the taxon to be indexed (or one of its synonyms)" )},
)
def portal_index_single_taxon():
    """
    ### Update the portal index for a single taxon - WFO ID passed in
    """
    @task()
    def fetch_taxon_graph_from_portal(**context):
        wfo_id = context["params"]["wfo_taxon_id"]

        # get a handle on the portal
        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))

        # data should be an array with a single taxon graph in it
        data = portal.getSingleTaxonGraph(wfo_id)
        if data: return data
        else: raise AirflowFailException("No data returned by portal")
    
    @task()
    def fetch_and_push(taxon_graph):

        fyllo = FylloApi(Variable.get("fyllo-api-url"), Variable.get("fyllo-api-token"))
        taxon_values = fyllo.fetchTaxonValues(taxon_graph)
        if not taxon_values: raise AirflowFailException("No data returned by Fyllo")

        portal = PortalApi(Variable.get("portal-api-url"), Variable.get("portal-api-token"))
        data = portal.pushTaxonValues(taxon_values)
        if not data: raise AirflowFailException("No data returned by portal")

    # dag wiring diagram
    fetch_and_push(fetch_taxon_graph_from_portal())

portal_index_single_taxon()


