import re
import json
import requests
from pprint import pprint
from requests.auth import HTTPBasicAuth

class SolrIndex:
    """An object to run queries against a SOLR index."""

    def __init__(self, solr_query_uri, wfo_default_version, solr_user, solr_password):
        self.query_uri = solr_query_uri
        self.default_version = wfo_default_version
        self.user = solr_user
        self.password = solr_password
        pass


    def getDoc(self, id):

        id = id.strip()

        # expand name ids to record ids
        if re.search(r'^wfo-[0-9]{10}$', id):
            record_id = id + '-' + self.default_version
        else:
            record_id = id

        # load it by id
        solr_query_uri = self.query_uri + '/get?id=' + record_id.strip()

        try:
            response = requests.get(solr_query_uri, auth=HTTPBasicAuth(self.user, self.password), timeout=10)
        except requests.exceptions.Timeout:
            print("Timed out")

        try:
            body = response.json()
            #print(body)
            if body['doc']:
                return body['doc']
            else:
                return None
        except Exception:
            return None


    def getSolrResponse(self, query):
       
        solr_query_uri = self.query_uri + '/query'

        response = self.postJson(solr_query_uri, json.dumps(query));
        
        return  response.json()                                                    
        
    
    def getSolrDocs(self, query):
       
        data = self.getSolrResponse(query)

        if "response" in data and "docs" in data["response"]:
            return data["response"]["docs"]
        else:
            return data

    def postJson(self, uri, json):
        response = requests.post(
            uri,
            data=json,
            headers={"Content-Type": "application/json"},
            auth=HTTPBasicAuth(self.user, self.password),
            timeout=10
            )
        return response
        