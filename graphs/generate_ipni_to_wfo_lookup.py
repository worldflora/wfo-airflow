import pendulum
from airflow.sdk import dag, task, Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
import os
import gzip
import csv
import json
import datetime
import re

# needed for macos running
os.environ['NO_PROXY'] = '*'

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "solstice", "lookup", "ipni"],
)
def generate_ipni_to_wfo_lookup():

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
           file_path = dir + '/015_ipni_to_wfo.csv.gz'

           with gzip.open(file_path, "wt", compresslevel=1) as csv_file:
                
                # use a writer with excel format
                csv_writer =  csv.writer(csv_file, dialect='excel')

                # write the header
                csv_writer.writerow([
                    "ipni_id",
                    "wfo_id"
                ])
    
                # write each line to the file

                # get the db connection for read
                mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
                conn = mysql_hook.get_conn()
                cursor = conn.cursor()
                cursor.execute("""SELECT distinct ipni.`value` as ipni_id, wfo.`value` as wfo_id
                    FROM `promethius`.identifiers as ipni
                    JOIN `promethius`.`names` as n on n.id = ipni.name_id
                    JOIN `promethius`.identifiers as wfo on n.prescribed_id = wfo.id 
                    WHERE ipni.kind = 'ipni'
                    AND wfo.kind = 'wfo'
                    AND n.`status` != 'deprecated'
                    ORDER BY ipni.`value`;""")

                # just write each row as is
                count = 0
                last_ipni = None
                for r in cursor:

                    # don't repeat any ipni ids
                    if r[0] == last_ipni:
                        continue

                    # don't write out any broken ones
                    if not r[0] or not re.match(r"^urn\:lsid\:ipni\.org\:names\:[0-9]+-[0-9]+$", r[0]):
                        continue 
                    
                    # we are good so write it out
                    csv_writer.writerow(r)
                    count = count + 1

                context["ti"].xcom_push(key="row_count", value=count)

                cursor.close()
                conn.close()    
                csv_file.close()
                
    @task
    def create_metadata(**context):

        dir  = context["ti"].xcom_pull(task_ids="create_output_directory", key="out_dir")
        file_path = dir + '/015_ipni_to_wfo.csv.gz'
        size_bytes = os.path.getsize(file_path)
        size_megabytes = round(size_bytes / 1048576, 2)

        file_path = file_path + '.json'

        count = context["ti"].xcom_pull(task_ids="create_csv", key="row_count")

        now = datetime.datetime.now()

        data = {
            "filename": "../www/downloads/lookup/015_ipni_to_wfo.csv.gz",
            "created": now.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "title": "The IPNIs we track mapped to WFO IDs",
            "description": f"A list of the {count:,} IPNI ID that we have linked to WFO IDs.",
            "size_bytes": size_bytes,
            "size_human": f"{size_megabytes}M"
        }

        with open(file_path, 'w', newline='') as json_file:
            json_file.write(json.dumps(data))
            json_file.close()


    create_output_directory() >> create_csv() >> create_metadata()

generate_ipni_to_wfo_lookup()