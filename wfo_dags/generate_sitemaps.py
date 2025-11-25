
import os
import pendulum
import gzip
import glob
from airflow.sdk import dag, task, Variable,  Param
from airflow.providers.mysql.hooks.mysql import MySqlHook

os.environ['NO_PROXY'] = '*' # needed to run on mac in dev environment.

BASE_URL = 'https://wfoplantlist.org/sitemaps/'

# make sure we have a directory to put the rss files in
base_dir = Variable.get("wfo-rhakhis-downloads-dir")
sitemaps_subdir = base_dir + "/sitemaps"
if not os.path.exists(sitemaps_subdir):
    os.makedirs(sitemaps_subdir)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "util"],
    params={"data_release_date":
             Param(default="2025-06-21", 
                   type="string",
                   title="Date Release Date:", 
                   description="The date of this data release (e.g. 2025-06-21). It will be used as the taxon identifier as well as the last mod date." )},
)
def generate_sitemaps():
    """
    ### generate_sitemaps
    """

    @task()
    def remove_old_files():
        files = glob.glob(sitemaps_subdir + '/*')
        for f in files:
            os.remove(f)


    @task()
    def write_sitemap_files(**context):

        # get the db connection
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        offset = 0
        page_size = 45000
        page_number = 0
        while True:

            offset = page_number * page_size

            cursor.execute("""SELECT i.`value` FROM `promethius`.`names` as n JOIN `promethius`.identifiers as i on n.prescribed_id = i.id and i.kind = 'wfo'
                WHERE n.`status` != 'deprecated' ORDER BY n.name_alpha LIMIT %s OFFSET %s""", (page_size, offset))
            rows = cursor.fetchall()

            if len(rows) == 0: break #escape the loop at the end of data

            # create the file
            file_path = f"{sitemaps_subdir}/wfoplantlist_{page_number}.xml.gz"
            with gzip.open(file_path, "wt") as file:
              
                # write the header
                file.write("""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                           """)
                            
                # loop the urls
                for row in rows:

                    date = context["params"]["data_release_date"]
                    taxon_id = row[0] + "-" + context["params"]["data_release_date"][:7]
                    file.write(f"""
    <url>
        <loc>https://www.wfoplantlist.org/taxon/{taxon_id}</loc>
        <lastmod>{date}</lastmod>
        <changefreq>yearly</changefreq>
    </url>                             
                               """)

                # write the footer
                file.write("""
</urlset> 
                           """)
                
                file.close()

                # on to the next page
                page_number += 1
    
    @task()
    def write_index_file(**context):

        now_str = context["params"]["data_release_date"]
        gz_files = glob.glob(sitemaps_subdir + '/*.xml.gz')

        # create the file
        file_path = f"{sitemaps_subdir}/sitemap.xml"
    
        with open(file_path, "w") as file:
            
            # write the header
            file.write("""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                        """)
                        
            # loop the urls
            for gz in gz_files:
                file_url =  BASE_URL  + os.path.basename(gz)
                file.write(f"""
<sitemap>
    <loc>{file_url}</loc>
    <lastmod>{now_str}</lastmod>
</sitemap>                            
                            """)

            # write the footer
            file.write("""
</sitemapindex>
                        """)
            
            file.close()

    remove_old_files() >> write_sitemap_files() >> write_index_file()

generate_sitemaps()

