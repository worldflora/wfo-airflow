class FacetExporter:
    ### Base class for facet exporters ###

    def __init__(self, file_path, table_name):
        self.file_path = file_path
        self.table_name = table_name


