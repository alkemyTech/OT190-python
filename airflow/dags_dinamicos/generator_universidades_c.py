import json
import os
import shutil
import fileinput
TEMPLATE_FILE = 'templates/template_universidades_c.py'
for filename in os.listdir('templates'):
    if filename.endswith('.json'):
        config  = json.load(open(f"templates/{filename}"))
        new_dagfile = f"{config['dag_id']}.py"
        if not os.path.exists(new_dagfile):  
            with open(new_dagfile, 'w') as fp:
                pass
        shutil.copyfile(TEMPLATE_FILE, new_dagfile)
        for line in fileinput.input(new_dagfile, inplace=True):
            line = line.replace("UNIVERSITY_HOLDER", config['university'])
            line = line.replace("DAG_ID_HOLDER", config['dag_id'])
            line = line.replace("SCHEDULE_INTERVAL_HOLDER", config['schedule_interval'])
            line = line.replace("SQL_FILEPATH_HOLDER", config["sql_filepath"])
            line = line.replace("SQL_FILENAME_HOLDER", config["sql_filename"])
            line = line.replace("CSV_FILENAME_HOLDER", config["csv_filename"])
            line = line.replace("S3_KEY_HOLDER", config["s3_key"])
            line = line.replace("LOAD_FILEPATH_HOLDER", config["load_filepath"])
            print(line, end="")
