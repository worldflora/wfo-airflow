from includes.api_base import ApiBase

class FylloApi(ApiBase):
    
    def fetchTaxonValues(self, taxon_graphs):
        data = self.doPost(taxon_graphs)
        return data
    
    def fetchSourceMetadata(self, modified_timestamp):
        data = self.doGet({
            'metadata' : 'sources',
            'since': modified_timestamp
            })
        return data
    
    def fetchFacetMetadata(self, modified_timestamp):
        data = self.doGet({
            'metadata' : 'facets',
            'since': modified_timestamp
            })
        return data
    
    def fetchScoresMetadata(self, modified_timestamp):
        data = self.doGet({
            'metadata' : 'scores',
            'since': modified_timestamp
            })
        return data
    
    def fetchSnippetsMetadata(self, modified_timestamp):
        data = self.doGet({
            'metadata' : 'snippets',
            'since': modified_timestamp
            })
        return data
