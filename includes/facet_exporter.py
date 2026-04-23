class FacetExporter:
    ### Base class for facet exporters ###

    def __init__(self, file_path, table_name, source_id):
        self.file_path = file_path
        self.table_name = table_name
        self.source_id = source_id
        


