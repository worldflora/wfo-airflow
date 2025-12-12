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
def generate_deduplicated_ids_lookup():

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
           file_path = dir + '/050_deduplicated_ids_lookup.csv.gz'

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
                        i.`value` as deduplicated_id,
                        pi.`value` as prescribed_id,
                        n.`name_alpha`,
                        n.`authors`,
                        n.`rank`,
                        n.`status`    
                    from `promethius`.identifiers as i
                    join `promethius`.`names` as n on n.id = i.name_id and i.kind = 'wfo'
                    join `promethius`.`identifiers` as pi on n.prescribed_id = pi.id and pi.kind = 'wfo'
                    where pi.id != i.id
                    order by name_alpha""")

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
        file_path = dir + '/050_deduplicated_ids_lookup.csv.gz'
        size_bytes = os.path.getsize(file_path)
        size_megabytes = round(size_bytes / 1048576, 2)

        file_path = file_path + '.json'

        count = context["ti"].xcom_pull(task_ids="create_csv", key="row_count")

        now = datetime.datetime.now()
        
        data = {
            "filename": "../www/downloads/lookup/050_deduplicated_ids_lookup.csv.gz",
            "created": now.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "title": "WFO IDs that have been deduplicated",
            "description": f"A list of the {count:,} WFO IDs that have been dedupliated and the prescribed ids that should be used in their place.",
            "size_bytes": size_bytes,
            "size_human": f"{size_megabytes}M"
        }
        
        with open(file_path, 'w', newline='') as json_file:
            json_file.write(json.dumps(data))
            json_file.close()


    create_output_directory() >> create_csv() >> create_metadata()

generate_deduplicated_ids_lookup()