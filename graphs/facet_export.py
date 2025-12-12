
import pendulum
from airflow.sdk import dag, task, Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import importlib.resources
from includes.solr import SolrIndex
import os
import re
import json
from pprint import pprint
from includes.facet_exporter_csv import FacetExporterCsv
from includes.facet_exporter_html import FacetExporterHtml
from includes.facet_exporter_coldp import FacetExporterColDp

# needed for macos running
os.environ['NO_PROXY'] = '*'

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "util", "facet"],
)
def facet_export():
    """
    ### Export data sources to a range of format files.
    """

    @task.short_circuit
    def fetch_next_job(**context):

        data_path = importlib.resources.path("includes.sql", "facet_export_next_job.sql")
        with open(data_path, encoding='utf-8') as fp:
            sqltxt = fp.read()

        # get the db connection
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        # run the query to get all the changed 
        cursor.execute(sqltxt)

        results = cursor.fetchall()
        if not results: print('no results')

        if len(results) > 0 :
            # get the names of the columns
            columns = [col[0] for col in cursor.description]
            row = dict(zip(columns, results[0]))

            # pop the job in memory for later
            context["ti"].xcom_push(key="job_row", value=row)
            context["ti"].xcom_push(key="source_id", value=row['data_source_id'])
            context["ti"].xcom_push(key="cache_table_name", value=f"export_cache_{row['data_source_id']}_{row['export_format']}")

            # carry on to the next operation
            return True 
        else:
            # declare we are done with the workflow as nothing to do
            return False

    @task()
    def flag_task_started(**context):
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        ds_row = context["ti"].xcom_pull(task_ids="fetch_next_job", key="job_row")
        job_id = ds_row['id']
        cursor.execute(f"UPDATE `wfo_facets`.`export_jobs` SET `status` = 1, `message` = 'In progress' WHERE id = {job_id};")
        conn.commit()

    @task()
    def flag_task_complete(**context):
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        ds_row = context["ti"].xcom_pull(task_ids="fetch_next_job", key="job_row")
        job_id = ds_row['id']
        cursor.execute(f"UPDATE `wfo_facets`.`export_jobs` SET `status` = 2, `message` = 'Complete' WHERE id = {job_id};")
        conn.commit()
    flag_task_complete_instance = flag_task_complete()
    
    @task(trigger_rule="none_failed_min_one_success")
    def drop_cache_data_table(**context):
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        table_name = context["ti"].xcom_pull(task_ids="fetch_next_job", key="cache_table_name")
        cursor.execute(f"DROP TABLE IF EXISTS `wfo_facets`.`{table_name}`;")
        print(table_name)

    drop_cache_data_table_at_start = drop_cache_data_table()
    drop_cache_data_table_at_end = drop_cache_data_table()
    
    @task    
    def create_cache_data_table(**context):
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        table_name = context["ti"].xcom_pull(task_ids="fetch_next_job", key="cache_table_name")
        cursor.execute(f"""CREATE TABLE `wfo_facets`.`{table_name}` (
                            `wfo_id` VARCHAR(14) NOT NULL,
                            `name` VARCHAR(100) NOT NULL,
                            `role` VARCHAR(10) NOT NULL,
                            `rank` VARCHAR(10) NOT NULL,
                            `parent_id` VARCHAR(14) NULL,
                            `path` TEXT NULL,
                            `featured` TINYINT NOT NULL DEFAULT 0,
                            `body_json` JSON NOT NULL,
                            PRIMARY KEY (`wfo_id`));""")

    @task
    def load_names_from_index(**context):

        # get an instance of the index to query
        version = Variable.get("wfo-default-classification")
        solr_uri = Variable.get("solr-index-uri")
        solr_user = Variable.get("solr-index-user")
        solr_password = Variable.get("solr-index-password")

        solr = SolrIndex(solr_uri, version, solr_user, solr_password)

        ds_row = context["ti"].xcom_pull(task_ids="fetch_next_job", key="job_row")
        # print(ds_row)
        source_id = ds_row['data_source_id']
        table_name = context["ti"].xcom_pull(task_ids="fetch_next_job", key="cache_table_name")

        # used to insert records
        insert_sql = f"""INSERT IGNORE INTO `wfo_facets`.`{table_name}` 
            (`wfo_id`, `name`,`role`,`rank`,`parent_id`,`path`,`featured`,`body_json`)
            VALUES 
            (%s, %s, %s,%s,%s,%s,%s,%s)"""

        sqltxt = f"SELECT wfo_id FROM wfo_facets.wfo_scores where source_id = {source_id} ORDER BY wfo_id;"
        
       
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
       
        # get the db connection for read
        conn1 = mysql_hook.get_conn()
        cursor1 = conn1.cursor()
        cursor1.execute(sqltxt)

        # get a second for write
        conn2 = mysql_hook.get_conn()
        cursor2 = conn2.cursor()

        for row in cursor1:
                
            # we are going to build a path from target back to root of tree
            path = []
            
            wfo = row[0]

            # we need to build a tree of docs even if this is a synonym - unlike what we do during indexing.
            target = solr.getDoc(wfo)

            # if we haven't got the target by the doc id maybe it is a deduplicated wfo_id?
            if not target:
                solr_query = {
                    'query' : f"wfo_id_deduplicated_ss:{wfo}",
                    'filter' : (f"classification_id_s:{version}")
                }
                solr_response = solr.getSolrResponse(solr_query)
                if 'response' in solr_response and "docs" in solr_response["response"]:
                    target = solr_response["response"]["docs"][0]

            # we should have a target from the datasource list here
            if not target: continue # do nothing if we didn't find anything
            
            path.append(target) # add it to the ones to be processed.

            # get all the records with this hierarchy  
            # if it has a path (not unplaced or deprecated)
            if "name_ancestor_path" in target:
                query = {
                    'query' : f"name_ancestor_path:{target['name_ancestor_path']}", # everything in this tree of names
                    "limit" : 10000, # big limit - not run out of memory theoretically could fail on stupid numbers of synonyms
                    'filter' : (f"classification_id_s:{target['classification_id_s']}") # filtered by this classification
                }
                docs = solr.getSolrDocs(query)

                # index them by their id
                all = {}
                for doc in docs:
                    all[doc["id"]] = doc

                # if the target is a synonym then we start the climb 
                # with the accepted name of that synonym
                if "accepted_id_s" in target:
                    path.append(all[target["accepted_id_s"]])

                # build the tree back to the root
                while True:
                    last = path[-1]
                    # if the last one has a parent and the parent is in the list of all then we add it
                    if "parent_id_s" in last and last['parent_id_s'] and last['parent_id_s'] in all:
                        path.append(all[last["parent_id_s"]])
                    else:
                        break

                # if they have to asked to include the synonyms then we must add them
                if "include_synonyms" in ds_row and ds_row['include_synonyms'] == 1:

                    # we add synonyms of the target (or its parent if it is a synonym)
                    if "accepted_id_s" in target:
                        syns_of = all[target["accepted_id_s"]]
                    else:
                        syns_of = target
                    
                    for syn in all.values():
                        if "accepted_id_s" in syn and syn["accepted_id_s"] == syns_of["id"]:
                            path.append(syn)       
                
            # work through the path and add them all to the db in that order
            for p in path:

                # the name path enables taxonomic sorting
                # unplaced and deprecated names don't have one
                name_path = None

                # work out the parent depending on if a synonym or not
                # because we are in the context of a single classification
                # we can use just the name wfo id not the fully qualified taxon id
                if p['role_s'] == 'accepted': 
                    if "parent_id_s" in p: parent_id = p['parent_id_s'][:14]
                    else: parent_id = None

                    # the name path for an accepted name is the one name path
                    name_path = p['name_path_s']
                
                if p['role_s'] == 'synonym': 
                    if "accepted_id_s" in p: parent_id = p['accepted_id_s'][:14]
                    else: parent_id = None

                    # synonyms have an appended name path otherwise it is the same as an accepted name
                    name_path  = p['name_path_s'] + '$' + p['full_name_string_alpha_s'].replace(' ', '$')
                    

                values = (
                    p['wfo_id_s'], # the wfo id of the name
                    p['full_name_string_plain_s'], # name for tracking
                    p['role_s'],
                    p['rank_s'],
                    parent_id, # calculated above
                    name_path, # calculated above
                    1 if p['wfo_id_s'] == target['wfo_id_s'] else 0, # featured
                    json.dumps(p) 
                    )

                cursor2.execute(insert_sql, values)
        
        # don't forget to commit or we have nothing.
        conn2.commit()

    @task
    def create_output_directory(**context):
        downloads_dir = Variable.get('wfo-facets-downloads-dir')
        source_id = context["ti"].xcom_pull(task_ids="fetch_next_job", key="source_id")

        out_dir = f"{downloads_dir}/{source_id}"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        
        context["ti"].xcom_push(key="out_dir", value=out_dir)

    @task.branch()
    def export_format_switch(**context):
        ds_row = context["ti"].xcom_pull(task_ids="fetch_next_job", key="job_row")
        if ds_row['export_format'] == 'csv': return 'export_csv_file'
        if ds_row['export_format'] == 'html': return 'export_html_file'
        if ds_row['export_format'] == 'coldp': return 'export_coldp_archive'
        return 'unsupported_export_format'
    
    export_format_switch_instance = export_format_switch()
    

    @task
    def export_csv_file(**context):
        
        table_name = context["ti"].xcom_pull(task_ids="fetch_next_job", key="cache_table_name")
        out_dir = context["ti"].xcom_pull(task_ids="create_output_directory", key="out_dir")
        ds_row = context["ti"].xcom_pull(task_ids="fetch_next_job", key="job_row") 
        ds_name = ds_row['data_source_name'] # name of datasource will be file name
        file_name = re.sub(r"[/\\?%*:|\"<>\x7F\x00-\x1F ]", "_", ds_name) # file safe the name
        file_path = f"{out_dir}/{file_name}.csv.gz" # full path to out file
        exporter = FacetExporterCsv(file_path, table_name)
        return exporter.runExport()

    @task
    def export_html_file(**context):
        
        table_name = context["ti"].xcom_pull(task_ids="fetch_next_job", key="cache_table_name")
        out_dir = context["ti"].xcom_pull(task_ids="create_output_directory", key="out_dir")
        ds_row = context["ti"].xcom_pull(task_ids="fetch_next_job", key="job_row") 
        ds_name = ds_row['data_source_name'] # name of datasource will be file name
        file_name = re.sub(r"[/\\?%*:|\"<>\x7F\x00-\x1F ]", "_", ds_name) # file safe the name
        file_path = f"{out_dir}/{file_name}.html.gz" # full path to out file

        exporter = FacetExporterHtml(file_path, table_name)
        return exporter.runExport(ds_row)
    
    @task
    def export_coldp_archive(**context):

        table_name = context["ti"].xcom_pull(task_ids="fetch_next_job", key="cache_table_name")
        out_dir = context["ti"].xcom_pull(task_ids="create_output_directory", key="out_dir")
        ds_row = context["ti"].xcom_pull(task_ids="fetch_next_job", key="job_row") 
        ds_name = ds_row['data_source_name'] # name of datasource will be file name
        file_name = re.sub(r"[/\\?%*:|\"<>\x7F\x00-\x1F ]", "_", ds_name) # file safe the name
        file_path = f"{out_dir}/{file_name}.coldp.zip" # full path to out file

        exporter = FacetExporterColDp(file_path, table_name)
        return exporter.runExport()


    @task
    def unsupported_export_format(**context):
        ds_row = context["ti"].xcom_pull(task_ids="fetch_next_job", key="job_row")
        print(f"Unsupported format: {ds_row['export_format']}")
    
    # build the graph
    fetch_next_job() >> flag_task_started() >> drop_cache_data_table_at_start >> create_cache_data_table() >> create_output_directory() >> load_names_from_index() >> export_format_switch_instance  
    export_format_switch_instance >> unsupported_export_format() >> drop_cache_data_table_at_end >> flag_task_complete_instance
    export_format_switch_instance >> export_csv_file() >> drop_cache_data_table_at_end >> flag_task_complete_instance
    export_format_switch_instance >> export_html_file() >> drop_cache_data_table_at_end >> flag_task_complete_instance
    export_format_switch_instance >> export_coldp_archive() >> drop_cache_data_table_at_end >> flag_task_complete_instance


facet_export()