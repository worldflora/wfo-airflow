from includes.api_base import ApiBase
import json

class PortalApi(ApiBase):

    def getTaxonGraphs(self):
        pass

    def getSingleTaxonGraph(self, wfo_id):
        data = self.doGet({'wfo_id': wfo_id})
        # print(str(data.keys()))
        if data and data['success'] and len(data['docs']) > 0 : return data['docs']
        else: return None

    def pushTaxonValues(self, taxon_values):
        data = self.doPost(taxon_values)
        return data
    
    def getTaxonGraphPage(self, page_size):
        data = self.doGet({'page_size': page_size})
        if data and data['success'] : return data['docs']
        else: return None