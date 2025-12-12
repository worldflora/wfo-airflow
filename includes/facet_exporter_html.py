
import gzip
import json
import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from includes.facet_exporter import FacetExporter

class FacetExporterHtml(FacetExporter):
    
    def runExport(self, ds_row):
        page_title = ds_row['data_source_name']
        now = datetime.datetime.now()
        export_date = now.strftime('%Y-%m-%dT%H:%M:%SZ')

        # create a file to put it in
        with gzip.open(self.file_path, "wt") as html_file:
        
            # write the header
            html_file.write(f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="ie=edge">
    <title>{page_title}</title>
    <style>
        body{{
            font-family: Arial, Helvetica, sans-serif;
        }}
        .featured{{
            color: green;
        }}
        .synonym{{
            font-weight: normal;
        }}
        .wfo-name-authors{{
            color: gray;
        }}
        ul {{
            list-style-type: none;
        }}
    </style>
    </head>
    <body>
    <h1>{page_title}</h1>
    <p>Exported {export_date}. Names highlighted in <span class="featured">green</span> are mentioned in the list, other names give their context within the current classification.</p>
    <h2>Classification</h2>
    <ul>
    """)
            
            common_ancestor_wfo = self.getCommonAncestorId(None)
            #html_file.write(common_ancestor_wfo)

            # get the db connection for read
            mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
            conn = mysql_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM `wfo_facets`.`{self.table_name}` ORDER BY `path`")
            columns = [col[0] for col in cursor.description]

            got_to_common_ancestor = False
            depth_current = 0
            in_synonym_section = False
            current_genus_name = None
            for r in cursor:
                row = dict(zip(columns, r))

                # skip rows till we get to the first child with multiple 
                if not got_to_common_ancestor:
                    if row['wfo_id'] == common_ancestor_wfo:
                        # found it!
                        got_to_common_ancestor = True
                        depth_current = row['path'].count('/') # this is the starting point for nested lists
                    else:
                        continue
                
                # step in the correct number of lists
                if depth_current < row['path'].count('/'):
                    for i in range(0, row['path'].count('/') - depth_current):
                        html_file.write('<ul>')

                # step out the correct number of lists
                if depth_current > row['path'].count('/'):
                    for i in range(0, depth_current - row['path'].count('/')):
                        html_file.write('</ul>')
                        current_genus_name = None # forget the genus name if we go up a level

                # we add an extra step in if we are moving into synonyms
                if not in_synonym_section and row['role'] == 'synonym':
                    in_synonym_section = True
                    html_file.write('<ul class="wfo-synonyms-section">')

                # we step out of synonym section when we get to a non synonym
                if in_synonym_section and row['role'] != 'synonym':
                    in_synonym_section = False
                    html_file.write('</ul>')
                    current_genus_name = None # forget the genus name if we go up a level

                # write out the actual name 
                html_file.write('<li>')
                current_genus_name = self.writeTaxonName(html_file, row, current_genus_name)
                html_file.write('</li>')

                # hold the depth for the next iteration
                depth_current = row['path'].count('/')

            # we are done working through the taxa
            cursor.close()
          
            # we need to close down any <ul> that we started 
            
            # if we are in a synonym section then close that
            if in_synonym_section:
                html_file.write('</ul>')
            
            # close each level we have stepped into
            for i in range(0, depth_current):
                html_file.write('</ul>')

            # write out the uplaced names
            html_file.write( "<h2>Unplaced Names</h2>")
            html_file.write("<p>Names that have not been placed in the classification by a WFO taxonomist yet.</p>")
        
            cursor = conn.cursor() 
            cursor.execute(f"SELECT * FROM `wfo_facets`.`{self.table_name}` where role = 'unplaced' order by `name`;")
            columns = [col[0] for col in cursor.description]
        
            html_file.write('<ul>')
            row_count = 0
            for r in cursor:
                row_count = row_count + 1
                row = dict(zip(columns, r))
                html_file.write('<li>')
                self.writeTaxonName(html_file, row, None)
                html_file.write('</li>')
            
            if row_count == 0:
                html_file.write('<li>None</li>')

            html_file.write('</ul>')
            cursor.close()


            # write out the deprecated names
            html_file.write( "<h2>Deprecated Names</h2>")
            html_file.write("<p>Names that can't be placed because they were created in error or represent an unused rank.</p>")
        
            cursor = conn.cursor() 
            cursor.execute(f"SELECT * FROM `wfo_facets`.`{self.table_name}` where role = 'deprecated' order by `name`;")
            columns = [col[0] for col in cursor.description]
        
            html_file.write('<ul>')
            row_count = 0
            for r in cursor:
                row_count = row_count + 1
                row = dict(zip(columns, r))
                html_file.write('<li>')
                self.writeTaxonName(html_file, row, None)
                html_file.write('</li>')
            
            if row_count == 0:
                html_file.write('<li>None</li>')

            html_file.write('</ul>')
            cursor.close()

            conn.close()
            html_file.write("""
        </body>
    </html>                            
    """)

        # close the file
        html_file.close()

        return True

    def getCommonAncestorId(self, wfo):

        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        # we don't have taxon id to work with
        if not wfo:
            cursor.execute(f"SELECT wfo_id FROM `wfo_facets`.`{self.table_name}` WHERE `rank` = 'code' and `parent_id` is null;")
            rows = cursor.fetchall()
            wfo = rows[0][0]
            cursor.close()

        # now we have one so look if it has more than one kid
        cursor = conn.cursor()
        cursor.execute(f"SELECT count(*) FROM `wfo_facets`.`{self.table_name}` WHERE `parent_id` = '{wfo}';")
        rows = cursor.fetchall()
        number_kids = rows[0][0]
        cursor.close()
        if number_kids > 1:
            # There are more than one child taxa so this is the root
            conn.close()
            return wfo
        else:
            # There is only one child so ask if that is the root
            cursor = conn.cursor()
            cursor.execute(f"SELECT wfo_id FROM `wfo_facets`.`{self.table_name}` WHERE `parent_id` = '{wfo}';")
            rows = cursor.fetchall()
            child_wfo = rows[0][0]
            cursor.close()
            conn.close()
            return self.getCommonAncestorId(child_wfo) 
        
    def writeTaxonName(self, html_file, row, current_genus_name):
            
            # get full data from the index
            json_data = json.loads(row['body_json'])

            css_class = 'featured' if row['featured'] else 'not-featured'
            css_class = css_class + " " + row['role']
            display_name = json_data['full_name_string_html_s']
            micro_citation = json_data['citation_micro_s'] if 'citation_micro_s' in json_data else ''

            # replace the genus name if there is one (we are below genus level)
            if 'genus_string_s' in json_data and (json_data['genus_string_s'] == current_genus_name or row['role'] == 'accepted'):
                genus_initial = json_data['genus_string_s'][0] + '.'
                display_name = display_name.replace(json_data['genus_string_s'], genus_initial)     
            
            if row['role'] == 'synonym': html_file.write(f'<strong>syn:&nbsp;</strong>')

            html_file.write(f'<strong class="{css_class}">{display_name}</strong>')
            html_file.write(f'&nbsp;[{json_data["rank_s"]}]&nbsp;')
            html_file.write(micro_citation)
            html_file.write(f'&nbsp;<a href="https://list.worldfloraonline.org/{json_data["wfo_id_s"]}" target="wfo_list">{json_data["wfo_id_s"]}</a>')
            html_file.write(f'&nbsp;[<a href="https://list.worldfloraonline.org/rhakhis/ui/index.html#{json_data["wfo_id_s"]}" target="rhakhis">Rhakhis</a>]')

            # we return the genus name we are in so we can abbreviate if needed
            return json_data['genus_string_s'] if 'genus_string_s' in json_data else None