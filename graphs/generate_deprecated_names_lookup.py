import pendulum
from airflow.sdk import dag, task, Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
import os
import gzip
import csv
import json
import datetime

# needed for macos running
os.environ['NO_PROXY'] = '*'

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "util", "facet"],
)
def generate_deprecated_names_lookup():

    @task
    def create_output_directory(**context):
        downloads_dir = Variable.get('wfo-rhakhis-downloads-dir')

        out_dir = f"{downloads_dir}/lookup"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        
        context["ti"].xcom_push(key="out_dir", value=out_dir)

    @task
    def create_csv(**context):
           
           dir  = context["ti"].xcom_pull(task_ids="create_output_directory", key="out_dir")
           file_path = dir + '/040_deprecated_names_lookup.csv.gz'

           with gzip.open(file_path, "wt") as csv_file:
                
                # use a writer with excel format
                csv_writer =  csv.writer(csv_file, dialect='excel')

                # write the header
                csv_writer.writerow([
                    'wfo_id',
                    'name_canonical',
                    'authors_string',
                    'rank',
                    'nomenclatural_status'
                ])
    
                # write each line to the file

                # get the db connection for read
                mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
                conn = mysql_hook.get_conn()
                cursor = conn.cursor()
                cursor.execute("""SELECT 
                    i.`value` as wfo_id, 
                    n.`name_alpha` as name_canonical,
                    n.`authors` as authors_string,
                    n.`rank` as 'rank',
                    n.`status` as 'nomenclatural_status'
                from `promethius`.`names` as n
                join `promethius`.identifiers as i on n.prescribed_id = i.id and i.kind = 'wfo'
                where n.`status` = 'deprecated'
                ORDER BY n.`name_alpha`""")

                # just write each row as is
                count = 0
                for r in cursor:
                    csv_writer.writerow(r)
                    count = count + 1

                context["ti"].xcom_push(key="row_count", value=count)

                cursor.close()
                conn.close()    
                csv_file.close()
                
    @task
    def create_metadata(**context):

        dir  = context["ti"].xcom_pull(task_ids="create_output_directory", key="out_dir")
        file_path = dir + '/040_deprecated_names_lookup.csv.gz'
        size_bytes = os.path.getsize(file_path)
        size_megabytes = round(size_bytes / 1048576, 2)

        file_path = file_path + '.json'

        count = context["ti"].xcom_pull(task_ids="create_csv", key="row_count")

        now = datetime.datetime.now()

        data = {
            "filename": "../www/downloads/lookup/040_deprecated_names_lookup.csv.gz",
            "created": now.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "title": "WFO IDs of deprecated name strings",
            "description": f"A list of the {count:,} names that we have deprecated.",
            "size_bytes": size_bytes,
            "size_human": f"{size_megabytes}M"
        }

        with open(file_path, 'w', newline='') as json_file:
            json_file.write(json.dumps(data))
            json_file.close()


    create_output_directory() >> create_csv() >> create_metadata()

generate_deprecated_names_lookup()