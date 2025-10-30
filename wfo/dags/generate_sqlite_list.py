
import os
import pendulum
import datetime
import csv
from airflow.sdk import dag, task, Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook

os.environ['NO_PROXY'] = '*' # needed to run on mac in dev environment.

# make sure we have a directory to put the rss files in
base_dir = Variable.get("wfo-rhakhis-downloads-dir")
sqlite_subdir = base_dir + "/sqlite"
if not os.path.exists(sqlite_subdir):
    os.makedirs(sqlite_subdir)


SQL_GET_CHILDREN = """
    USE promethius;
    SELECT  
        t.id as taxon_db_id,
        i.`value` as wfo,
        if(t.taxon_name_id = tn.id, 'accepted', 'synonym') as 'role', # work out the role in the classification.
        pi.`value` as parent_wfo,
        bi.`value` as basionym_wfo,
        ii.`value` as ipni_id,
        n.`rank` as 'rank_string',
        n.`name` as 'name_string', 
		n.*,
		t.is_hybrid,
		t.is_fossil
	FROM taxa as t 
	JOIN taxon_names as tn on t.id = tn.taxon_id 
    JOIN `names` as n on n.id = tn.name_id
    JOIN `identifiers` as i on n.prescribed_id = i.id and i.kind = 'wfo'
    JOIN taxa as pt on t.parent_id = pt.id
    JOIN taxon_names as ptn on pt.taxon_name_id = ptn.id
    JOIN `names` as pn on ptn.name_id = pn.id
    JOIN identifiers as pi on pn.prescribed_id = pi.id and pi.kind = 'wfo'
    LEFT JOIN identifiers as ii on n.preferred_ipni_id = ii.id and ii.kind = 'ipni'
    LEFT JOIN `names` as bn on n.basionym_id = bn.id
    LEFT join identifiers as bi on bn.prescribed_id = bi.id and bi.kind = 'wfo'
"""

SQL_GET_UNPLACED = """
    USE promethius;
    SELECT  
        null as taxon_db_id,
        i.`value` as wfo,
        'unplaced' as 'role', # work out the role in the classification.
        null as parent_wfo,
        bi.`value` as basionym_wfo,
        ii.`value` as ipni_id,
        n.`rank` as 'rank_string',
        n.`name` as 'name_string', 
		n.*,
		NULL AS is_hybrid,
		NULL AS is_fossil
    FROM `names` as n 
    JOIN `identifiers` as i on n.prescribed_id = i.id and i.kind = 'wfo'
    LEFT JOIN identifiers as ii on n.preferred_ipni_id = ii.id and ii.kind = 'ipni'
    LEFT JOIN `names` as bn on n.basionym_id = bn.id
    LEFT join identifiers as bi on bn.prescribed_id = bi.id and bi.kind = 'wfo'
    LEFT JOIN taxon_names as tn on n.id = tn.name_id
    WHERE  n.genus = 'GENUS_NAME'
    and tn.id is null
    AND n.`status` != 'deprecated'
    ORDER BY n.name_alpha
"""

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "util"],
)
def generate_sqlite_list():
    """
    ### generate_sqlite_list
    """

    def write_header(csv_writer):
        csv_writer.writerow([
            'wfo',
            'full_name',
            'role',
            'micro_citation',
            'parent_wfo',
            'basionym_wfo',
            'name_string',
            'genus',
            'species_epithet',
            'subspecific_epithet',
            'full_name_html'
        ])

    def write_row(row, csv_writer):
        csv_writer.writerow([

            
        ])

    def write_rows(parent_row, cursor, csv_writer):

        if not parent_row:
            # we are on the root taxon
            sql = f"{SQL_GET_CHILDREN}  WHERE t.parent_id = t.id "
        else:
            # we are on a descendent
            parent_db_id = parent_row[0]
            sql = f"{SQL_GET_CHILDREN} WHERE t.parent_id = {parent_db_id} AND t.parent_id != t.id ORDER BY `role`, name_alpha"

        # fetch the rows
        cursor.execute(sql)
        while cursor.nextset():
            results = cursor.fetchall()
            if not results: continue

        # if we are on the first one the write the column headings
        if not parent_row:
            columns = [col[0] for col in cursor.description]
            csv_writer.writerow(columns)

        # work through the rows and write them out + their kids
        for row in results:
            csv_writer.writerow(row) # this name
            if row[2] == 'accepted' :
                write_rows(row, cursor, csv_writer) # all its children
                if row[6] == 'genus' and row[7]:
                    # get the unplaced
                    sql = SQL_GET_UNPLACED.replace('GENUS_NAME', row[7])
                    cursor.execute(sql)
                    while cursor.nextset():
                        unplaced = cursor.fetchall()
                        if not unplaced: continue
                    for r in unplaced:
                        csv_writer.writerow(r)

    @task
    def build_csv_file():

        # create the csv file
        csv_file_path = f"{sqlite_subdir}/plant_list_sqlite.csv"
        with open(csv_file_path, "w", newline='') as csv_file:

            csv_writer = csv.writer(csv_file, dialect='excel')

            # get the db connection
            mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
            conn = mysql_hook.get_conn()
            cursor = conn.cursor()
            
            write_rows(None, cursor, csv_writer)

            csv_file.close()

    @task
    def create_sqlite_db():
        pass

    @task
    def import_csv_file():
        pass
    
    @task
    def remove_csv_file():
        pass

    @task 
    def add_indexes():
        pass

    @task 
    def add_metadata_table():
        pass

    @task 
    def compress_sqlite_db():
        pass


    build_csv_file() >> create_sqlite_db() >> import_csv_file() >> remove_csv_file() >> add_indexes() >> add_metadata_table() >> compress_sqlite_db()

generate_sqlite_list()

