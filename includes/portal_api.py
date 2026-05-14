from includes.api_base import ApiBase
from airflow.sdk import Variable
import json

class PortalApi(ApiBase):

    def getSingleTaxonGraph(self, wfo_id):
        data = self.doGet({'wfo_id': wfo_id, 'classification': Variable.get('wfo-default-classification')})
        # print(str(data.keys()))
        if data and data['success'] and len(data['docs']) > 0 : return data['docs']
        else: return None

    def pushTaxonValues(self, taxon_values):
        data = self.doPost(taxon_values)
        return data
    
    def getTaxonGraphPage(self, page_size):
        data = self.doGet({'page_size': page_size, 'classification': Variable.get('wfo-default-classification')})
        if data and data['success'] : return data['docs']
        else: return None

    def getFacetMetadataLastModified(self):
        data = self.doGet({'last_modified': 'wfo-facet'})
        if data and data['success'] : return data['value']
        else: return None

    def getFacetMetadataLastModified(self):
        data = self.doGet({'last_modified': 'wfo-facet'})
        if data and data['success']: 
            last_mod = data['value']
            return last_mod
        # not got a value so return none
        return None
    
    def pushFacetMetadata(self, metadata):
        data = self.doPost(metadata)
        return data

    def getSourceMetadataLastModified(self):
        # there are facet sources and snippet sources in the index but they are updated by a single call on the fyllo side
        last_mod = 0

        # get the facet source last update time
        data = self.doGet({'last_modified': 'wfo-facet-source', 'classification': Variable.get('wfo-default-classification')})
        if data and data['success']: 
            last_mod = data['value']
            # get the snippet source last update time 
            data = self.doGet({'last_modified': 'wfo-snippet-source', 'classification': Variable.get('wfo-default-classification')})
            if data and data['success']:
                if data['value'] > last_mod: last_mod = data['value']
                return last_mod
            
        # not got a value so return none
        return None
    
    def pushSourceMetadata(self, metadata):
        data = self.doPost(metadata)
        return data
    
    def deleteScoresMetadata(self):
        data = self.doGet({'delete': 'wfo-facet-value-score'})
        return data

    def getScoresMetadataLastModified(self):
        data = self.doGet({'last_modified': 'wfo-facet-value-score'})
        if data and data['success']: 
            last_mod = data['value']
            return last_mod
        # not got a value so return none
        return None
    
    def pushScoresMetadata(self, metadata):
        data = self.doPost(metadata)
        return data
    
    def deleteSnippetsMetadata(self):
        data = self.doGet({'delete': 'wfo-snippet'})
        return data

    def getSnippetsMetadataLastModified(self):
        data = self.doGet({'last_modified': 'wfo-snippet'})
        if data and data['success']: 
            last_mod = data['value']
            return last_mod
        # not got a value so return none
        return None
    
    def pushSnippetsMetadata(self, metadata):
        data = self.doPost(metadata)
        return data
