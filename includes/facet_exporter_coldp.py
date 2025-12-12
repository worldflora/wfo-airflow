import csv
import json
import pathlib
from airflow.providers.mysql.hooks.mysql import MySqlHook
from includes.facet_exporter import FacetExporter

class FacetExporterColDp(FacetExporter):
    
    def runExport(self):

        # we will use the dir to build the csv files in
        path = pathlib.Path(self.file_path)
        self.file_dir = str(path.parent)

        # we run across the names in the cache multiple
        # times, once for each of the tables we are building
        self.buildNamesCsv()
        self.buildRefsCsv()
        self.buildTaxaCsv()
        self.buildSynonymsCsv()
        self.buildMaterialCsv()
        
        return True
    
    def buildNamesCsv(self):

        with open(self.file_dir + '/name.tsv', 'w', newline='') as csv_file:
            csv_writer =  csv.writer(csv_file, delimiter='\t')

            # write the header
            csv_writer.writerow([
                "ID",
                "alternativeID",
                "basionymID",
                "scientificName",
                "authorship",
                "rank",
                "uninomial",
                "genus",
                "infragenericEpithet",
                "specificEpithet",
                "infraspecificEpithet",
                "code",
                "referenceID",
                "publishedInYear",
                "link"
            ])

            # get the db connection for read
            mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
            conn = mysql_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM `wfo_facets`.`{self.table_name}` ORDER BY `path`")
            columns = [col[0] for col in cursor.description]

            for r in cursor:

                row = dict(zip(columns, r))
                json_data = json.loads(row['body_json']) # this is the full index doc

                csv_row = []

                # for ID we use the name ID
                csv_row.append(row['wfo_id'])

                # alternativeID = identifiers_other_value
                alt_ids = []
                if 'identifiers_other_value_ss' in json_data:   
                    for id in json_data['identifiers_other_value_ss']:
                        alt_ids.append(id)
                csv_row.append(','.join(alt_ids))

                # basionymID
                if 'basionym_id_s' in json_data:
                    csv_row.append(json_data['basionym_id_s'][:14])
                else:
                    csv_row.append(None)

                # scientificName,
                csv_row.append(json_data['full_name_string_plain_s'])
                
                # authorship,
                csv_row.append(json_data['authors_string_s'] if 'authors_string_s' in json_data else None)
                
                # rank
                csv_row.append(json_data['rank_s'] if 'rank_s' in json_data else None)
                
                # uninomial" - if we are at genus or above
                if 'genus_string_s' not in json_data:
                    csv_row.append(json_data['name_string_s'])
                else:
                    csv_row.append(None)

                # genus - will only be set for below genus names
                csv_row.append(json_data['genus_string_s'] if 'genus_string_s' in json_data else None)

                # infragenericEpithet
                # we are below genus (we have a genus string) 
                # but we are not below species (we don't have a species epithet)
                # and we are not at species level
                if 'genus_string_s' in json_data and 'species_string_s' not in json_data and json_data['rank_s'] != 'species':
                    csv_row.append(json_data['name_string_s'])
                else:
                    csv_row.append(None)

                # specificEpithet 
                # we are below genus (we have a genus string) 
                # we either 
                if 'genus_string_s' in json_data:
                    if 'species_string_s' in json_data:
                        csv_row.append(json_data['species_string_s'])
                    else:
                        # we are at species level so it is the name string
                        csv_row.append(json_data['name_string_s'])
                else:
                    csv_row.append(None)

                # infraspecificEpithet
                # we have a genus and a species part to the actual name is the subspecific part
                if 'genus_string_s' in json_data and 'species_string_s' in json_data:
                    csv_row.append(json_data['name_string_s'])
                else:
                    csv_row.append(None)

                # code
                csv_row.append('botanical')

                # referenceID
                # we cheat and make a reference that is the microcitation later
                if 'citation_micro_s' in json_data and json_data['citation_micro_s'] != 'Unknown' and json_data['citation_micro_s'] != '-':
                    csv_row.append(json_data['wfo_id_s'] + '_mc')
                else:
                    csv_row.append(None)

                # publishedInYear
                csv_row.append(json_data['publication_year_i'] if 'publication_year_i' in json_data else None)

                # link
                csv_row.append('https://list.worldfloraonline.org/' + json_data['wfo_id_s'])

                csv_writer.writerow(csv_row)

        # close the file
        csv_file.close()

        return True


    def buildRefsCsv(self):

        with open(self.file_dir + '/reference.tsv', 'w', newline='') as csv_file:
            csv_writer =  csv.writer(csv_file, delimiter='\t')

            # write the header
            csv_writer.writerow([
                "ID",
                "citation",
                "link",
                "doi",
                "remarks"
            ])


            # get the db connection for read
            mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
            conn = mysql_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM `wfo_facets`.`{self.table_name}` ORDER BY `path`")
            columns = [col[0] for col in cursor.description]

            for r in cursor:

                row = dict(zip(columns, r))
                json_data = json.loads(row['body_json']) # this is the full index doc

                # add the microcitation
                if "citation_micro_s" in json_data:
                    csv_row.append()


                csv_writer.writerow(csv_row)

    
    def buildTaxaCsv(self):
        pass

    def buildSynonymsCsv(self):
        pass

    def buildMaterialCsv(self):
        pass


    