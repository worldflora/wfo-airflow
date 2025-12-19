import os
import time
import pendulum
from datetime import datetime
import glob
import zipfile
from importlib import resources
import subprocess
from airflow.sdk import dag, task, Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowFailException

os.environ['NO_PROXY'] = '*' # needed to run on mac in dev environment.

# make sure we have a directory to put the rss files in
base_dir = Variable.get("wfo-rhakhis-downloads-dir")
rss_subdir = base_dir + "/rss"
if not os.path.exists(rss_subdir):
    os.makedirs(rss_subdir)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "legacy", "solstice", "util"],
)
def leg_gen_dwc_family_files():
    """ This will work through all the families and call the php script 
        that generates the files for that family.
        It is a legacy animation dag 
    """
    
    @task
    def process_families(**context):

        # get a list of all the families and work through them
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        script_directory = Variable.get("wfo-rhakhis-api-scripts-dir")
        data_directory = Variable.get("wfo-rhakhis-downloads-dir")
        data_directory = data_directory + '/dwc'

        # run the query to get all the changed 
        cursor.execute("""
            SELECT i.`value` as wfo, n.`name` 
            FROM `promethius`.`names` as n 
            JOIN `promethius`.`identifiers` as i on n.prescribed_id = i.id
            JOIN `promethius`.`taxon_names` as tn on n.id = tn.name_id
            JOIN `promethius`.`taxa` as t on t.taxon_name_id = tn.id
            where n.`rank` = 'family'
            order by n.name_alpha
                       """)
        
        columns = [col[0] for col in cursor.description]
        
        for r in cursor:
            row = dict(zip(columns, r))
            print(f"{row['name']}\t{row['wfo']}")

            file_path = f"{data_directory}/{row['name']}_{row['wfo']}.zip"

            # if the file exists and it was created in the last 6 hours don't recreate it
            if os.path.exists(file_path) and os.path.getmtime(file_path) > time.time() - (60*60*6):
                print('Exists so skipping')
                continue

            # call the bash script
            command = f"cd {script_directory} && php -d memory_limit=2048M gen_family_dwc_file.php {row['wfo']}"
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            if result.returncode:
                print(result.stdout)
            else:
                print('Created OK')
    @task
    def validate_family_links(**context):
        ### this could be ported to python ###

        script_directory = Variable.get("wfo-rhakhis-api-scripts-dir")
        command = f"cd {script_directory} && php gen_family_dwc_validate.php"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(result.stdout)
        if result.returncode:
            print(result.stderr)
            raise AirflowFailException("Failed to validate links in files. See errors above")
        
    @task
    def fetch_family_placement_previous(**context):
        
        # there is a list of the previous family placements in the ../data/sources directory
        # file pattern is _emonocot_family_placement_2025-06.csv.zip
        # we need the last one of these copied and unzipped to be
        # _emonocot_family_placement_previous.csv
        # in the downloads directory

        # get the last file
        script_directory = Variable.get("wfo-rhakhis-api-scripts-dir")
        parent_directory = os.path.dirname(script_directory)
        pattern = parent_directory + "/data/sources/_emonocot_family_placement_????-??.csv.zip"
        files = glob.glob(pattern)
        files.sort()
        last_file = files[-1]

        # unzip that to the data dir
        data_directory = Variable.get("wfo-rhakhis-downloads-dir")
        data_directory = data_directory + '/dwc'

        with zipfile.ZipFile(last_file, 'r') as zip_ref:
            zip_ref.extractall(data_directory)

        # rename it to the previous one
        old_file_path = data_directory + '/_emonocot_family_placement_current.csv'
        new_file_path = data_directory + '/_emonocot_family_placement_previous.csv'
        os.rename(old_file_path, new_file_path)
    
    @task
    def validate_family_families(**context):
        ### this could be ported to python ###

        script_directory = Variable.get("wfo-rhakhis-api-scripts-dir")
        command = f"cd {script_directory} && php -d memory_limit=10G gen_family_dwc_validate_families.php"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(result.stdout)
        print(result.returncode)
        if result.returncode:
            print(result.stderr)
            raise AirflowFailException("Failed to validate families in files. See errors above")
        
    @task
    def store_family_placement_current(**context):
        
        # there is a list of the previous family placements in the ../data/sources directory
        # file pattern is _emonocot_family_placement_2025-06.csv.zip
        # we need the last one of these copied and unzipped to be
        # _emonocot_family_placement_previous.csv
        # in the downloads directory

        # get the last file
        script_directory = Variable.get("wfo-rhakhis-api-scripts-dir")
        parent_directory = os.path.dirname(script_directory)

        year = datetime.now().strftime('%Y') 
        month = datetime.now().strftime('%m') 

        destination_zip = f"{parent_directory}/data/sources/_emonocot_family_placement_{year}-{month}.csv.zip"

        data_directory = Variable.get("wfo-rhakhis-downloads-dir")
        data_directory = data_directory + '/dwc'

        zip = zipfile.ZipFile(destination_zip, "w", zipfile.ZIP_DEFLATED)
        zip.write( f"{data_directory}/_emonocot_family_placement_current.csv", "_emonocot_family_placement_current.csv")
        zip.close()

        os.remove(f"{data_directory}/_emonocot_family_placement_current.csv")
        os.remove(f"{data_directory}/_emonocot_family_placement_previous.csv")
        
    
    @task
    def remove_existing_combined_file(**context):

        data_directory = Variable.get("wfo-rhakhis-downloads-dir")
        file_path = data_directory + '/dwc/families_dwc.tar.gz'

        # If file exists, delete it.
        if os.path.isfile(file_path): 
            os.remove(file_path)
            print(f"Removed {file_path}")
        else:
            # If it fails, inform the user.
            print(f"No need to remove file {file_path}")

    @task
    def create_new_combined_file(**context):

        data_directory = Variable.get("wfo-rhakhis-downloads-dir")
        data_directory = data_directory + '/dwc'
        command = f"cd {data_directory} && tar -czvf families_dwc.tar.gz *_wfo-*.zip"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(result.stdout)
        print(result.returncode)
        if result.returncode:
            print(result.stderr)
            raise AirflowFailException("Failed to remove old file.")       

    # build the graph
    process_families() >> validate_family_links() >> fetch_family_placement_previous() >> validate_family_families() >> store_family_placement_current() >>remove_existing_combined_file() >> create_new_combined_file()

leg_gen_dwc_family_files()


