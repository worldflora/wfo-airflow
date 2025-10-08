
import os
import pendulum
import datetime
from airflow.sdk import dag, task, Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook

os.environ['NO_PROXY'] = '*' # needed to run on mac in dev environment.

# make sure we have a directory to put the rss files in
base_dir = Variable.get("wfo-rhakhis-downloads-dir")
rss_subdir = base_dir + "/rss"
if not os.path.exists(rss_subdir):
    os.makedirs(rss_subdir)

@dag(
    schedule="14 08 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wfo", "util"],
)
def generate_rss():
    """
    ### Generate Family RSS
    
    """

    def write_out_file(dir_name, file_name, rows):
                
        # create the order dir if it doesn't exit
        order_dir = f"{rss_subdir}/{dir_name}"
        if not os.path.exists(order_dir):
            os.makedirs(order_dir)

        # make a file for family
        rss_file = f"{order_dir}/{file_name}.xml"

        # if we don't have rows to write then we won't overwrite what is there
        # but we will create an empty file if one doesn't exist.
        if(len(rows) == 0 and os.path.exists(rss_file)): return

        with open(rss_file, "w") as file:
            now = datetime.datetime.now()
            
            # header
            header = '''<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">

	<title><![CDATA[{0}]]></title>
	<subtitle>Latest changes in Rhakhis belonging to {0}</subtitle>
	<id>https://rhakhis.rbge.info/rhakhis/api/downloads/rss/{3}/{1}.xml</id>
    <link href="https://rhakhis.rbge.info/rhakhis/api/downloads/rss/index.html" />
    <link rel="self" href="https://rhakhis.rbge.info/rhakhis/api/downloads/rss/{3}/{1}.xml" />
	<updated>{2}</updated>
'''.format(file_name, file_name, now.strftime('%Y-%m-%dT%H:%M:%SZ'), dir_name)

            file.write(header)
            # entries
            for row in rows:
                entry = '''
	<entry>
		<title><![CDATA[{0} {7} ({1})]]></title>
		<link href="https://list.worldfloraonline.org/rhakhis/ui/index.html#{1}" />
		<id>https://list.worldfloraonline.org/rhakhis/ui/index.html#{1}</id>
        <published>{2}</published>
		<updated>{3}</updated>
		<summary><![CDATA[{4}]]></summary>
		<author>
			<name><![CDATA[{5}]]></name>
            <uri>https://orcid.org/{6}</uri>
		</author>
	</entry>
'''.format(
        row['name_alpha'], # title - full name
        row['wfo'], # wfo id
        f"{row['created']}".replace(' ', 'T') + 'Z', # created as published
        f"{row['modified']}".replace(' ', 'T') + 'Z', # modified as updated
        row['change_log'],
        row['user_name'],
        row['user_orcid'],
        row['authors'],
        row['rank']
        )   
                file.write(entry)

            # footer
            file.write("\n</feed>")

            file.close()

    @task
    def update_rss_files():

        current_order = None
        current_family = None
        
        # we gather the rows for file we will output
        order_rows = []
        family_rows = []
        all_rows = []

        data_dir = os.path.join(os.path.dirname(__file__), 'sql')
        data_path = os.path.join(data_dir, 'modified_by_order_family.sql')
        with open(data_path, encoding='utf-8') as fp:
            sqltxt = fp.read()

        # get the db connection
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        # run the query to get all the changed 
        cursor.execute(sqltxt)
        while cursor.nextset():
            results = cursor.fetchall()
            if not results: continue

            # get the names of the columns
            columns = [col[0] for col in cursor.description]


            for r in results:
                row = dict(zip(columns, r))

                # get clean names for order and family
                order = row['order']
                if order == None: order = 'unplaced'
                family = row['family']
                if family == None: family = 'unplaced'

                # initialise the first order and family 
                if not current_order: current_order = order
                if not current_family: current_family = family

                # move to the next order so write the last one out and initialise the next
                if order != current_order:
                     # order is in a file called its own name in the folder callled its own name
                     write_out_file(current_order, current_order, order_rows)
                     current_order = order
                     order_rows = []

                # move to the next family so write the last one out and initialise the next
                if family != current_family:
                     write_out_file(current_order, current_family, family_rows)
                     current_family = family
                     family_rows = []

                # add the row to the order and family output
                order_rows.append(row)
                family_rows.append(row)
                all_rows.append(row)

        # write out the last order and family
        write_out_file(current_order, current_order, order_rows)
        write_out_file(current_order, current_family, family_rows)
        write_out_file('all', 'all', all_rows)

    @task
    def update_index():

        data_dir = os.path.join(os.path.dirname(__file__), 'sql')
        data_path = os.path.join(data_dir, 'all_orders_families.sql')
        with open(data_path, encoding='utf-8') as fp:
            sqltxt = fp.read()

        # get the db connection
        mysql_hook = MySqlHook(mysql_conn_id="airflow_wfo")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        # run the query to get all the changed 
        cursor.execute(sqltxt)
        while cursor.nextset():
            results = cursor.fetchall()
            if not results: continue

            # start the index file
            with open(rss_subdir + '/index.html', "w") as file:


                header = '''<!DOCTYPE html>
<html>
<head>
<title>RSS Feeds for Rhakhis</title>
</head>
<body>
<div style='display: block; float: right; max-width: 16em; margin: 1em;'>
    <a href="https://tettris.eu/" target="tettris"><img src="../../images/tettris.png" width="100%" /></a>
</div>
<h1>RSS Feeds for Rhakhis</h1>
<p>
    These are the RSS (Atom 1.0) feeds of recently changed names in the Rhakhis taxonomic editor.
    Records changed in the last 30 days or since the system was initiated are included.
    This service was developed as part of a project funded by <a href="https://tettris.eu/">TETTRIs</a>. 
</p>
'''
                file.write(header)

                file.write(f'<p>All changes <a href="all/all.xml"><img src="../../images/feed-icon.png" width="18px" /></a></p>')
                
                # get the names of the columns
                columns = [col[0] for col in cursor.description]

                current_order = None
                for r in results:
                    row = dict(zip(columns, r))

                    # order files when we have new orders
                    if current_order != row['order']:
                        write_out_file(row['order'], row['order'], [])

                        if current_order != None: file.write("</ul>\n")

                        current_order = row['order'] # flag we are starting a new one

                        #start a new ul for the order
                        file.write(f'\n<h2>{row["order"]}&nbsp;')
                        file.write(f'<a href="{row["order"]}/{row["order"]}.xml"><img src="../../images/feed-icon.png" width="18px" /></a>')
                        file.write('</h2>\n')
                        file.write('<ul>\n')

                    # make sure there is a file for every family
                    write_out_file(row['order'], row['family'], [])
                    file.write(f'\n<li>{row["family"]}&nbsp;')
                    # feed icon as link
                    file.write(f'<a href="{row["order"]}/{row["family"]}.xml"><img src="../../images/feed-icon.png" width="18px" /></a>')
                    file.write(f'</li>\n')
                footer = '''
</ul>
</body>
</html>
'''
                file.write(footer)
                file.close()

    update_rss_files() >> update_index()

generate_rss()


