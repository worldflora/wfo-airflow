from includes.api_base import ApiBase

class FylloApi(ApiBase):
    
    def fetchTaxonValues(self, taxon_graphs):
        data = self.doPost(taxon_graphs)
        return data