import os
from airflow.models import Variable


# SINGER_DATA = Variable.get("SINGER_DATA")
SINGER_DATA = './singer_data/'
# cmd=f"python {SINGER_DATA}csv_gsheet.py | ~/.virtualenvs/target-gsheet/bin/target-gsheet -c {SINGER_DATA}gsheet_config.json >> {SINGER_DATA}state.json"
cmd = f"python {SINGER_DATA}tap-csv.py | ~/.virtualenvs/target-postgres/bin/target-postgres --config {SINGER_DATA}target_postgres_config.json >> {SINGER_DATA}state.json"
# cmd="python csv_gsheet.py | ~/.virtualenvs/target-postgres/bin/target-postgres --config target_postgres_config.json >> state.json"

os.system(cmd)

