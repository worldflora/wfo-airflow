import csv
import gzip
import json
from airflow.providers.mysql.hooks.mysql import MySqlHook
from includes.facet_exporter import FacetExporter

class FacetExporterCsv(FacetExporter):
    
    def runExport(self):
         # create a file to put it in
        #with open(file_path, 'w', newline='') as csv_file:
        with gzip.open(self.file_path, "wt") as csv_file:
            
            # use a writer with excel format
            csv_writer =  csv.writer(csv_file, dialect='excel')
            
            # write the header
            csv_writer.writerow([
                'wfo_id',
                'scientific_name',
                'taxonomic_status',
                'named_in_list',
                'rank',
                'parent_id',
                'accepted_id',
                'name_path',
                'name_no_authors',
                'authors',
                'micro_citation',
                'nomenclatural_status'
            ])
    
            # write each line to the file
            
            # get the db connection for read
            mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
            conn = mysql_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM `wfo_facets`.`{self.table_name}` ORDER BY `path`")
            columns = [col[0] for col in cursor.description]
                
            for r in cursor:

                row = dict(zip(columns, r))
    
                csv_row = []
                csv_row.append(row['wfo_id'])
                csv_row.append(row['name'])
                csv_row.append(row['role'])
                csv_row.append(row['featured'])
                csv_row.append(row['rank'])

                # convert to the conventional way of doing 
                # parent and accepted name links
                if row['parent_id']:
                    if row['role'] == 'accepted':
                        csv_row.append(row['parent_id'])
                        csv_row.append(None)
                    else:
                        csv_row.append(None)
                        csv_row.append(row['parent_id'])
                else:
                    csv_row.append(None)
                    csv_row.append(None)

                csv_row.append(row['path'])

                # now some fluff from the rest of the record
                json_data = json.loads(row['body_json'])
                csv_row.append(json_data['full_name_string_no_authors_plain_s'] if 'full_name_string_no_authors_plain_s' in json_data else None)
                csv_row.append(json_data['authors_string_s'] if 'authors_string_s' in json_data else None)
                csv_row.append(json_data['citation_micro_t'] if 'citation_micro_t' in json_data else None)
                csv_row.append(json_data['nomenclatural_status_s'] if 'nomenclatural_status_s' in json_data else None)
                
                csv_writer.writerow(csv_row)

        # close the file
        csv_file.close()

        return True
