import src.etl_scripts.utils as utils
import src.etl_scripts.queries as queries
import os
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)

password_db = os.getenv("DB_ROOT_PASSWORD")
earthquakes = utils.Load("earthquakes", password_db)
earthquakes.create_db()

print('CREATE DB ------------\n', 'Start creation of Data Model DM \n')

print('\n  ---> creating earthquakes table')
earthquakes.create_insert_table(queries.create_table_earthquakes)

print('\n  ---> creating gdp table')
earthquakes.create_insert_table(queries.create_table_gdp)

print('\n  ---> creating link table')
earthquakes.create_insert_table(queries.create_link_table)

print('\n End creation of Data Model DM \n')



