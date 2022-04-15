import pandas as pd

import src.etl_scripts.utils as utils

import os
from dotenv import load_dotenv
from pathlib import Path
from tqdm import tqdm

def date_to_string(date):
    date_str = str(date)
    t = date[:10]
    z = date[11:19]
    return t+ ' ' +z

def fstr(template):
    return eval(f"f'{template}'")


print('INSERT DATA INTO DB ------------ \n' ,'Start insert into data base from parquet files \n')

dotenv_path = Path("../.env")
load_dotenv(dotenv_path=dotenv_path)

password_db = os.getenv("DB_ROOT_PASSWORD")

print('  ---> connection into db')
earthquakes = utils.Load("earthquakes", password_db)

# ----------------------------------- GDP INGESTION ---------------------------------------------------------

print('\n ------- GDP INGESTION ------- \n', '\n  ---> reading f_gdp.parquet file\n')
f_gdp = pd.read_parquet('../data/extraction_layer/f_gdp.parquet', engine='fastparquet')

# automatic insert
fields_param_lst = []
fields_lst = []
for ele in f_gdp.columns:
    if f_gdp[ele].dtypes == 'object':
        fields_param_lst.append("{quote}"+'{row["'+f"{ele}"+'"]}'+"{quote}")
    else:
        fields_param_lst.append('{row["'+f"{ele}"+'"]}')
    fields_lst.append(ele)

fields_param = ", ".join(fields_param_lst)
fields = ", ".join(fields_lst)

print('\n  ---> start inserting data - GDP\n')
list_id_already_db = []
counter = 0
for index, row in tqdm(f_gdp.iterrows(), total=f_gdp.shape[0]):

    quote = '"'

    # creating the query to insert values into table
    query_insert = "INSERT INTO earthquakes.f_gdp ({fields}) VALUES ({fields_param});"
    query_insert_eval = fstr(fstr(query_insert)).replace("nan", "null")

    id_link = earthquakes.get_id(f'{row["id_gdp"]}', "id_gdp", "id_gdp", "f_gdp")

    if id_link == 'id is not in DB':
        earthquakes.create_insert_table(query_insert_eval)

    else:
        counter += 1
        row_already_db = 'id: ' + str({row['id_gdp']}) + " is already in DB"
        list_id_already_db.append(row_already_db)


ids_already_db = ".\n ".join(list_id_already_db)

# print(counter, 'id_earthquake were already in DB')
# print(ids_already_db)
# print(counter, 'id_earthquake were already in DB')

print('\n  ---> finish inserting data - GDP \n')

# ----------------------------------- LINK TABLE INGESTION ---------------------------------------------------------

print('\n ------- LINK TABLE INGESTION ------- \n', '\n  ---> reading link_eq_gdp.parquet file\n')
link_table = pd.read_parquet('../data/extraction_layer/link_eq_gdp.parquet', engine='fastparquet')

# automatic insert
fields_param_lst = []
fields_lst = []
for ele in link_table.columns:
    if link_table[ele].dtypes == 'object':
        fields_param_lst.append("{quote}"+'{row["'+f"{ele}"+'"]}'+"{quote}")
    else:
        fields_param_lst.append('{row["'+f"{ele}"+'"]}')
    fields_lst.append(ele)

fields_param = ", ".join(fields_param_lst)
fields = ", ".join(fields_lst)

print('\n  ---> start inserting data - Link Table\n')
list_id_already_db = []
counter = 0
for index, row in tqdm(link_table.iterrows(), total=link_table.shape[0]):

    quote = '"'

    # creating the query to insert values into table
    query_insert = "INSERT INTO earthquakes.link_table ({fields}) VALUES ({fields_param});"
    query_insert_eval = fstr(fstr(query_insert)).replace("nan", "null")

    id_link = earthquakes.get_id(f'{row["key_earthquake_gdp"]}', "key_earthquake_gdp", "key_earthquake_gdp", "link_table")

    if id_link == 'id is not in DB':
        earthquakes.create_insert_table(query_insert_eval)

    else:
        counter += 1
        row_already_db = 'id: ' + str({row['key_earthquake_gdp']}) + " is already in DB"
        list_id_already_db.append(row_already_db)


ids_already_db = ".\n ".join(list_id_already_db)

# print(counter, 'key_earthquake_gdp were already in DB')
# print(ids_already_db)
# print(counter, 'key_earthquake_gdp were already in DB')

print('\n  ---> finish inserting data - GDP \n')

# ----------------------------------- EARTHQUAKES INGESTION ---------------------------------------------------------
print('\n ------- EARTHQUAKES INGESTION ------- \n','\n  ---> reading f_earthquakes.parquet file\n')
f_earthquakes = pd.read_parquet('../data/extraction_layer/f_earthquakes_after1995_ml.parquet', engine='fastparquet')


print('\n  ---> formatting updated_date and time\n')
f_earthquakes['updated_date']=f_earthquakes.apply(lambda my_data: date_to_string(my_data['updated_date']), axis = 1)
f_earthquakes['time']=f_earthquakes.apply(lambda my_data: date_to_string(my_data['time']), axis = 1)

# automatic insert
fields_param_lst = []
fields_lst = []
for ele in f_earthquakes.columns:
    if f_earthquakes[ele].dtypes == 'object':
        fields_param_lst.append("{quote}"+'{row["'+f"{ele}"+'"]}'+"{quote}")
    else:
        fields_param_lst.append('{row["'+f"{ele}"+'"]}')
    fields_lst.append(ele)


fields_param = ", ".join(fields_param_lst)
fields = ", ".join(fields_lst)


print('\n  ---> start inserting data - earthquakes\n')
list_id_already_db = []
counter = 0
for index, row in tqdm(f_earthquakes.iterrows(), total=f_earthquakes.shape[0]):

    quote = '"'

    # creating the query to insert values into table
    query_insert = "INSERT INTO earthquakes.f_earthquakes ({fields}) VALUES ({fields_param});"
    query_insert_eval = fstr(fstr(query_insert)).replace("nan", "null")

    id_link = earthquakes.get_id(f'{row["id_earthquake"]}', "id_earthquake", "id_earthquake", "f_earthquakes")

    if id_link == 'id is not in DB':
        earthquakes.create_insert_table(query_insert_eval)
    else:
        counter += 1
        row_already_db = 'id: ' + str({row['id_earthquake']}) + " is already in DB"
        list_id_already_db.append(row_already_db)

ids_already_db = ".\n ".join(list_id_already_db)

# print(counter, 'id_earthquake were already in DB')
# print(ids_already_db)
# print(counter, 'id_earthquake were already in DB')

print('  ---> finish inserting data - earthquakes \n')
print('Finish insert into data base from parquet files \n')