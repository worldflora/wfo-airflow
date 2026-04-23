import csv
import re
import hashlib
import json
import pathlib
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from includes.facet_exporter import FacetExporter

# https://github.com/CatalogueOfLife/coldp?tab=readme-ov-file


class FacetExporterColDp(FacetExporter):

    according_to = {} # dictionary of accordingTo ref ids for output during taxon csv write
    contributor_people = {}
    contributor_tens = {}
    
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
        self.buildMetadataFile()


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
                    csv_row.append(json_data['wfo_id_s'] + '_protologue')
                else:
                    csv_row.append(None)

                # publishedInYear
                csv_row.append(json_data['publication_year_i'] if 'publication_year_i' in json_data else None)

                # link
                csv_row.append('https://list.worldfloraonline.org/' + json_data['wfo_id_s'])

                csv_writer.writerow(csv_row)

                # while we are running through all the names we can keep tabs on the
                # contributors for output in the metadata
                self.addContributor(json_data)

        # close the file
        csv_file.close()

        return True
    
    def addContributor(self, json_data):

        # there is only ever one TEN set
        if 'ten_name_s' in json_data and 'ten_uri_s' in json_data:
            self.contributor_tens[json_data['ten_name_s']] = {
              'url': json_data['ten_uri_s'],
              'organisation': json_data['ten_name_s'],
              'note': "WFO Taxonomic Expert Network"
            }

        # there may be multiple editors
        if 'editors_name_ss' in json_data and 'editors_orcid_ss' in json_data:

            for i in range(0, len(json_data['editors_name_ss'])):

                # we need to fangle with the name string to get it in the right format
                parts = json_data['editors_name_ss'][i].strip().split(' ')
                family = parts[-1].title()
                given = ' '.join(parts[:-1])
                given = given.replace('.', '. ')
                sort = f"{family}, {given}"

                # FIXME Uppercase the first letter of hyphenated names
                self.contributor_people[sort] = {
                    'family': family,
                    'given' : given,
                    'sort' : sort,
                    'orcid' : json_data['editors_orcid_ss'][i],
                    'note' : f"WFO taxonomic expert"
                }

    def buildRefsCsv(self):

        already_done = [] # track the ones we have already written out

        with open(self.file_dir + '/reference.tsv', 'w', newline='') as csv_file:
            
            csv_writer =  csv.writer(csv_file, delimiter='\t')

            self.according_to.clear() # used to pass links to taxon writer

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

                # what references are we interested in writing out?

                # We need protologs for names  
                # add the microcitation as the protologue - we may have a better one
                # in the references but it isn't guaranteed to be correct.
                if "citation_micro_s" in json_data:
                    csv_row = []
                    csv_row.append(json_data['wfo_id_s'] + '_protologue') # use the name id as the id for the micro citation
                    csv_row.append(json_data['citation_micro_s']) # citation
                    csv_row.append(None) # link
                    csv_row.append(None) # doi
                    csv_row.append('Micro-citation of protologue.') # remarks
                    csv_writer.writerow(csv_row)

                # We need the accordingTo for a name that is placed as a synonym or a taxon
                # we can fetch that and store the id for use when writing out the taxon or synonym
                # next later
                # (could go up the tree to find taxon source but won't as chances are that is included in
                # data anyhow. but could be enhanced later with this.)

                if "reference_contexts_ss" in json_data and 'taxonomic' in json_data['reference_contexts_ss']:
                    
                    # pick the first one (they should only have one?)
                    pos = json_data['reference_contexts_ss'].index('taxonomic')

                    # make up a hash to use as an id so we don't repeat the uri and confuse people
                    # Encode the string to bytes
                    data_bytes = json_data['reference_uris_ss'][pos].encode('utf-8')

                    # Create an MD5 hash object and update it with the bytes
                    md5_hasher = hashlib.md5()
                    md5_hasher.update(data_bytes)

                    # Get the hexadecimal representation of the hash
                    ref_key = md5_hasher.hexdigest()

                    # if we haven't already added it we add it
                    if json_data['reference_uris_ss'][pos] not in already_done:
                        csv_row = []
                        csv_row.append(ref_key) # hash of uri as key
                        csv_row.append(json_data['reference_labels_ss'][pos]) # citation
                        csv_row.append(json_data['reference_uris_ss'][pos]) # link

                        # fill the DOI if we have one embedded in the uri
                        matches = re.search('^https:\/\/doi.org\/(.*)', json_data['reference_uris_ss'][pos], flags=re.IGNORECASE)
                        if matches is not None:
                            doi = matches.group(0)
                            csv_row.append(doi) # doi
                        else:
                            csv_row.append(None) # doi

                        csv_row.append('Taxonomic treatment followed.') # remarks
    
                        csv_writer.writerow(csv_row)
                    
                        # note that we have done it.
                        already_done.append(json_data['reference_uris_ss'][pos])

                    # add mapping between name and reference to the according_to list
                    self.according_to[json_data['wfo_id_s']] = ref_key

        # close the file
        csv_file.close()

        return True

    
    def buildTaxaCsv(self):
            
        with open(self.file_dir + '/taxon.tsv', 'w', newline='') as csv_file:
        
            csv_writer =  csv.writer(csv_file, delimiter='\t')

            # write the header
            csv_writer.writerow([
                "ID",
                "nameID",
                "parentID",
                "accordingToID",
                "link"
            ])

            # get the db connection for read
            mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
            conn = mysql_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM `wfo_facets`.`{self.table_name}` ORDER BY `path`")
            columns = [col[0] for col in cursor.description]

            # get a list of the according to references
            for r in cursor:

                row = dict(zip(columns, r))
                json_data = json.loads(row['body_json']) # this is the full index doc

                # only do accepted taxa - we are building the hierarchy.
                if json_data["role_s"] != 'accepted': continue
                
                csv_row = []

                # ID
                csv_row.append(json_data['wfo_id_s']) # use the name id 
                csv_row.append(json_data['wfo_id_s']) # nameID
                if 'parent_id_s' in json_data:
                    csv_row.append(json_data['parent_id_s'][:14]) # parentID - just the 10 digit part of it
                else:
                    csv_row.append(None)            
                
                if json_data['wfo_id_s'] in self.according_to:
                    csv_row.append(self.according_to[json_data['wfo_id_s']])  # accordingToID 
                else:
                    csv_row.append(None)

                csv_row.append(f"https://list.worldfloraonline.org/{json_data['wfo_id_s']}") # link

                # write the row
                csv_writer.writerow(csv_row)
        
        # close the file
        csv_file.close()
        return True

    def buildSynonymsCsv(self):
         
         with open(self.file_dir + '/synonym.tsv', 'w', newline='') as csv_file:
        
            csv_writer =  csv.writer(csv_file, delimiter='\t')
            # write the header
            csv_writer.writerow([
                "ID",
                "taxonID",
                "nameID",
                "accordingToID",
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

                # only do synonyms
                if json_data["role_s"] != 'synonym': continue

                # to prevent confusion make up a primary key by hashing the wfo id
                md5_hasher = hashlib.md5()
                md5_hasher.update(json_data['wfo_id_s'].encode('utf-8'))
                key = md5_hasher.hexdigest()

                csv_row.append(key) # "ID",
                csv_row.append(json_data['accepted_id_s'][:14]) # "taxonID",
                csv_row.append(json_data['wfo_id_s']) # "nameID",
                
                # "accordingToID",
                if json_data['wfo_id_s'] in self.according_to:
                    csv_row.append(self.according_to[json_data['wfo_id_s']])  # accordingToID 
                else:
                    csv_row.append(None)
                
                # "link"
                csv_row.append(f"https://list.worldfloraonline.org/{json_data['wfo_id_s']}") # link
                
                    # write the row
                csv_writer.writerow(csv_row)
        
            # close the file
            csv_file.close()
            return True           

    def buildMetadataFile(self):
        
        metadata = {}
        metadata["license"] = "cc0"


        # this will need to be edited by the user in ChecklistBank
        metadata["creator"] =[
            {
                "email": "taxwg@worldfloraonline.org",
                "url": "http://www.worldfloraonline.org",
                "organisation": "World Flora Online"
            }
        ]

        # get the facts about the datasource and facets
        sql = f"""SELECT 
            f.`name` as facet_name,
            f.`description` as facet_description,
            f.`link_uri` as facet_uri,
            fv.`name` as facet_value_name,
            fv.`description` as facet_value_description,
            fv.`link_uri` as facet_value_uri,
            s.`name` as source_name,
            s.`description` as source_description,
            s.`link_uri` as source_uri,
            ss.`category` as snippet_category,
            ss.`language` as snippet_language
        FROM wfo_facets.sources as s
        JOIN wfo_facets.facet_value_sources AS fvs on s.id = fvs.source_id
        JOIN wfo_facets.facet_values as fv on fv.id = fvs.facet_value_id
        JOIN wfo_facets.facets as f on f.id = fv.facet_id
        LEFT JOIN wfo_facets.snippet_sources as ss on ss.source_id = s.id
        WHERE s.id = {self.source_id};"""

        # get the db connection for read
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        r = cursor.fetchone()
        meta_row = dict(zip(columns, r))

        metadata["title"] = f"{meta_row['source_name']}"
        metadata["description"] = f"{meta_row['source_description']}\n{meta_row['source_name']}\n{meta_row['facet_name']}\n{meta_row['facet_description']}\n{meta_row['facet_value_name']}\n{meta_row['facet_value_description']}"

        # version and issue based on current time.
        metadata["version"] = datetime.now().strftime("%Y-%m-%d")
        metadata["issued"] = metadata["version"]

        # put in all the contributors
        metadata['contributors'] = []

        # people
        people_sorted = dict(sorted(self.contributor_people.items()))
        for key in people_sorted:
            metadata['contributors'].append(people_sorted[key])

        # TENs
        tens_sorted = dict(sorted(self.contributor_tens.items()))
        for key in tens_sorted:
            metadata['contributors'].append(tens_sorted[key])

        # write it out as a json metadata file
        with open(self.file_dir + '/metadata.json', "w") as f:
            json.dump(metadata, f, indent=4)

        # depending on what kind of facet / snippet this we now
        # export different types of data.

        # FIXME do we do this from the index of the db?

        # if it is a vernacular snippet then we create a vernacular table
        if meta_row['snippet_category'] == 'vernacular':
            self.buildVernacularFile(meta_row)

        # if it is a ???? then we create a distribution table

        # otherwise we create a taxon property?


    def buildVernacularFile(self, meta_row):

        # get the db connection for read
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        # get just the taxa - they are the ones decorated with the info
        cursor.execute(f"SELECT * FROM `wfo_facets`.`{self.table_name}` WHERE `role` = 'accepted' ORDER BY `path`")
        columns = [col[0] for col in cursor.description]

        for r in cursor:
            row = dict(zip(columns, r))
            json_data = json.loads(row['body_json']) # this is the full index doc


    def buildDistributionFile(self, meta_row):
        pass

    def buildTaxonPropertiesFile(self, meta_row):
        pass


        
