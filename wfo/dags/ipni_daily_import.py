import pendulum
import requests
import os
import csv
from airflow.sdk import dag, task, Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

IPNI_URI = "https://storage.googleapis.com/ipni-data/ipniWebName.csv.xz"

os.environ['NO_PROXY'] = '*' # needed to run on mac in dev environment.

INSERT_SQL = """INSERT INTO `kew`.`ipni_new`
    (
        `id`,
        `authors_t`,
        `basionym_s_lower`,
        `basionym_author_s_lower`,
        `lookup_basionym_id`,
        `bibliographic_reference_s_lower`,
        `bibliographic_type_info_s_lower`,
        `reference_collation_s_lower`,
        `collection_date_as_text_s_lower`,
        `collection_day_1_s_lower`,
        `collection_day_2_s_lower`,
        `collection_month_1_s_lower`,
        `collection_month_2_s_lower`,
        `collection_number_s_lower`,
        `collection_year_1_s_lower`,
        `collection_year_2_s_lower`,
        `collector_team_as_text_t`,
        `lookup_conserved_against_id`,
        `lookup_correction_of_id`,
        `date_created_date`,
        `date_last_modified_date`,
        `distribution_s_lower`,
        `east_or_west_s_lower`,
        `family_s_lower`,
        `taxon_scientific_name_s_lower`,
        `taxon_sci_name_suggestion`,
        `genus_s_lower`,
        `geographic_unit_as_text_s_lower`,
        `hybrid_b`,
        `hybrid_genus_b`,
        `lookup_hybrid_parent_id`,
        `hybrid_parents_s_lower`,
        `infra_family_s_lower`,
        `infra_genus_s_lower`,
        `infraspecies_s_lower`,
        `lookup_isonym_of_id`,
        `lookup_later_homonym_of_id`,
        `latitude_degrees_s_lower`,
        `latitude_minutes_s_lower`,
        `latitude_seconds_s_lower`,
        `locality_s_lower`,
        `longitude_degrees_s_lower`,
        `longitude_minutes_s_lower`,
        `longitude_seconds_s_lower`,
        `name_status_s_lower`,
        `name_status_bot_code_type_s_lower`,
        `name_status_editor_type_s_lower`,
        `nomenclatural_synonym_s_lower`,
        `lookup_nomenclatural_synonym_id`,
        `north_or_south_s_lower`,
        `original_basionym_s_lower`,
        `original_basionym_author_team_s_lower`,
        `original_hybrid_parentage_s_lower`,
        `original_remarks_s_lower`,
        `original_replaced_synonym_s_lower`,
        `original_taxon_distribution_s_lower`,
        `lookup_orthographic_variant_of_id`,
        `other_links_s_lower`,
        `lookup_parent_id`,
        `publication_s_lower`,
        `lookup_publication_id`,
        `publication_year_i`,
        `publication_year_full_s_lower`,
        `publication_year_note_s_lower`,
        `publishing_author_s_lower`,
        `rank_s_alphanum`,
        `reference_t`,
        `reference_remarks_s_lower`,
        `remarks_s_lower`,
        `lookup_replaced_synonym_id`,
        `lookup_same_citation_as_id`,
        `score_s_lower`,
        `species_s_lower`,
        `species_author_s_lower`,
        `lookup_superfluous_name_of_id`,
        `suppressed_b`,
        `top_copy_b`,
        `lookup_type_id`,
        `type_locations_s_lower`,
        `type_name_s_lower`,
        `type_remarks_s_lower`,
        `type_chosen_by_s_lower`,
        `type_note_s_lower`,
        `detail_author_team_ids`,
        `detail_species_author_team_ids`,
        `page_as_text_s_lower`,
        `citation_type_s_lower`,
        `lookup_validation_of_id`,
        `version_s_lower`,
        `powo_b`,
        `sortable`,
        `family_taxon_name_sortable`,
        `ipni_wfo_id`
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s 
    );"""

@dag(
    schedule="40 03 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "ipni", "kew"],
    template_searchpath=['../includes/']
)
def ipni_daily_import():
    """
    ### Will import the latest IPNI 
    Downloads IPNI database dump and keeps a copy in our own
    table with the previously match names.
    """

    create_database = SQLExecuteQueryOperator(
        task_id="create_database",
        conn_id="airflow_wfo",
        sql="CREATE DATABASE IF NOT EXISTS `kew`;"
    )

    @task
    def download_file(**context):

        base = Variable.get("local-file-storage")
        subdir = base + "/ipni"
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        file_path = subdir + "/new.csv.xz"
        print(file_path)
        print(IPNI_URI)

        response = requests.get(IPNI_URI)
        with open(file_path, "wb") as file:
            file.write(response.content)

        return file_path

    @task.bash()
    def expand_xz_file(file_path):
        print(file_path)
        return f"unxz -f {file_path}"

    drop_new_data_table = SQLExecuteQueryOperator(
        task_id="drop_new_data_table",
        conn_id="airflow_wfo",
        sql="DROP TABLE IF EXISTS `kew`.`ipni_new`;"
    )

    create_new_data_table = SQLExecuteQueryOperator(
        task_id="create_new_data_table",
        conn_id="airflow_wfo",
        sql="sql/ipni.sql"
    )

    @task
    def import_table_rows(**context):
       
       # get the db connection
       mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
       conn = mysql_hook.get_conn()
       cursor = conn.cursor()

       # get the file connection
       xz_file_path = context["ti"].xcom_pull(task_ids="download_file", key="return_value")
       csv_file_path = os.path.splitext(xz_file_path)[0] # first item in tuple

       # run through that file
       with open(csv_file_path, newline='') as csv_file:
           csv_reader =  csv.reader(csv_file, delimiter='|')
           next(csv_reader) # skip header
           for row in csv_reader:
                
                # 61 is  publication_year_i which is now and integer
                if not row[61].isnumeric():
                    row[61] = None

                # remove the timezones from the dates
                row[19] = row[19].split('+', 1)[0]
                row[20] = row[20].split('+', 1)[0]
                
                # actually insert - update upoi
                cursor.execute(INSERT_SQL, row)

       conn.commit()

    check_same_or_more_rows = SQLCheckOperator(
        task_id="check_same_or_more_rows",
        conn_id="airflow_wfo",
        sql="SELECT (SELECT count(*) from `kew`.`ipni_new`)>=(SELECT count(*) from `kew`.`ipni`) AS is_same_or_bigger;"
    )

    drop_old_table = SQLExecuteQueryOperator(
        task_id="drop_old_table",
        conn_id="airflow_wfo",
        sql="DROP TABLE IF EXISTS `kew`.`ipni`;"
    )

    rename_new_table = SQLExecuteQueryOperator(
        task_id="rename_new_table",
        conn_id="airflow_wfo",
        sql="RENAME TABLE `kew`.`ipni_new` to `kew`.`ipni`;"
    )

    @task
    def remove_csv_file(**context):

        # get the file connection
        xz_file_path = context["ti"].xcom_pull(task_ids="download_file", key="return_value")
        csv_file_path = os.path.splitext(xz_file_path)[0] # first item in tuple

        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)
        
    # the actual process
    create_database >> expand_xz_file(download_file()) >> drop_new_data_table >> create_new_data_table >> import_table_rows() >> check_same_or_more_rows >> drop_old_table >> rename_new_table >> remove_csv_file()

dag = ipni_daily_import()

if __name__ == "__main__":
    dag.test()
