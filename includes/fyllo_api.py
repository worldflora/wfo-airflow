from includes.api_base import ApiBase

class FylloApi(ApiBase):
    
    def fetchTaxonValues(self, taxon_graphs):
        return self.doPost(taxon_graphs)
    
    def fetchSourceMetadata(self, modified_timestamp):
        return self.doGet({
            'metadata' : 'sources',
            'since': modified_timestamp
            })
    
    def fetchFacetMetadata(self, modified_timestamp):
        return self.doGet({
            'metadata' : 'facets',
            'since': modified_timestamp
            })
    
    def fetchScoresMetadata(self, modified_timestamp):
        return self.doGet({
            'metadata' : 'scores',
            'since': modified_timestamp
            })
    
    def fetchSnippetsMetadata(self, modified_timestamp):
        return self.doGet({
            'metadata' : 'snippets',
            'since': modified_timestamp
            })
    
    def fetchNextImportJob(self):
        return self.doGet({
            'import': 'next'
        })
    
    def pageImportJob(self, data):
        return self.doGet(data)
