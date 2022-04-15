import pandas as pd
import uuid


def get_year(time):
 time = str(time)
 year = time[0:4]
 return int(year)


def date_to_string(date):
 date_str = str(date)
 t = date[:10]
 z = date[11:19]
 return t + ' ' + z


print('EXTRACT SOURCE ------------ \n', 'Start Data Extraction \n')

# Extracting gdp data from csv and managing data for inserted into db

print('  ---> Ingesting gdp from csv file \n')
f_gdp_raw = pd.read_csv("../data/02.fred_gdp_usa.csv")


print('  ---> creating id_gdp for gdp data \n')
f_gdp_raw['id_gdp']=f_gdp_raw.apply(lambda my_data: str(my_data['year'])+str(my_data['latitude'])+str(my_data['longitude']), axis = 1)
f_gdp_raw['id_gdp']=f_gdp_raw.apply(lambda my_data: str(uuid.uuid5(uuid.NAMESPACE_DNS, my_data['id_gdp'])), axis = 1)

print('  ---> creating key_gdp_earthquake for gdp data \n')
f_gdp_raw['key_gdp_earthquake']=f_gdp_raw.apply(lambda my_data: str(my_data['year'])+str(my_data['latitude'])+str(my_data['longitude']), axis = 1)
f_gdp_raw['key_gdp_earthquake']=f_gdp_raw.apply(lambda my_data: str(uuid.uuid5(uuid.NAMESPACE_DNS, my_data['key_gdp_earthquake'])), axis = 1)

print('  ---> Dropping columns for gdp data \n')
f_gdp_raw = f_gdp_raw.drop(columns=['Unnamed: 0'])

print('  ---> Sorting columns for gdp data \n')

column_order = ['id_gdp', 'key_gdp_earthquake', 'year', 'gdp', 'state_code', 'state_name', 'longitude', 'latitude']
f_gdp_raw = f_gdp_raw.reindex(columns=column_order)

print('  ---> Saving gdp data into parquet \n')
f_gdp_raw.to_parquet('../data/extraction_layer/f_gdp.parquet', engine='fastparquet')
f_gdp_raw.to_csv('../data/extraction_layer/f_gdp.csv')

# Extracting earthquake data from csv and managing data for inserted into db

print('  ---> Ingesting earthquake from csv file \n')
f_earthquakes_raw = pd.read_csv("../data/consolidated_data.csv")

f_earthquakes_raw['year'] = f_earthquakes_raw['time'].apply(get_year)

print('  ---> creating id_earthquake for earthquake data \n')
f_earthquakes_raw['id_earthquake']=f_earthquakes_raw.apply(lambda my_data: str(my_data['time'])+str(my_data['latitude'])+str(my_data['longitude']), axis = 1)
f_earthquakes_raw['id_earthquake']=f_earthquakes_raw.apply(lambda my_data: str(uuid.uuid5(uuid.NAMESPACE_DNS, my_data['id_earthquake'])), axis = 1)

print('  ---> creating key_earthquake_gdp for earthquake data \n')
f_earthquakes_raw['key_earthquake_gdp']=f_earthquakes_raw.apply(lambda my_data: str(my_data['year'])+str(my_data['latitude'])+str(my_data['longitude']), axis = 1)
f_earthquakes_raw['key_earthquake_gdp']=f_earthquakes_raw.apply(lambda my_data: str(uuid.uuid5(uuid.NAMESPACE_DNS, my_data['key_earthquake_gdp'])), axis = 1)

print('  ---> Dropping columns for earthquake data \n')
f_earthquakes_raw = f_earthquakes_raw.drop(columns=['Unnamed: 0'])

print('  ---> Sorting columns for earthquake data \n')
column_order = ['id_earthquake', 'key_earthquake_gdp', 'time', 'year', 'latitude', 'longitude',
 'depth', 'mag', 'magType', 'nst', 'gap', 'dmin', 'rms', 'net', 'id', 'place',
 'type', 'horizontalError', 'depthError', 'magError', 'magNst', 'status',
 'locationSource', 'magSource', 'updated']
f_earthquakes_raw = f_earthquakes_raw.reindex(columns=column_order)

print('  ---> Renaming columns for earthquake data \n')
columns_renaming = {"updated":"updated_date"}

f_earthquakes_raw.rename(columns=columns_renaming, inplace=True)

print('  ---> Filtering earthquake data \n')
filter_mlmd_cond = (f_earthquakes_raw["magType"] == 'ml') | (f_earthquakes_raw["magType"] == 'Ml') | (f_earthquakes_raw["magType"] == 'md') | (f_earthquakes_raw["magType"] == 'Md')
filter_md_cond = (f_earthquakes_raw["magType"] == 'md') | (f_earthquakes_raw["magType"] == 'Md')
filter_ml_cond = (f_earthquakes_raw["magType"] == 'ml') | (f_earthquakes_raw["magType"] == 'Ml')
year_to_filter = '2004'

f_earthquakes_raw_after1995_mlmd = f_earthquakes_raw[(f_earthquakes_raw["time"] > f'{year_to_filter}-01-01T00:00:00.000Z') & filter_mlmd_cond]
f_earthquakes_raw_before1995_mlmd = f_earthquakes_raw[(f_earthquakes_raw["time"] < f'{year_to_filter}-01-01T00:00:00.000Z') & filter_mlmd_cond]

f_earthquakes_raw_after1995_md = f_earthquakes_raw[(f_earthquakes_raw["time"] > f'{year_to_filter}-01-01T00:00:00.000Z') & filter_md_cond]
f_earthquakes_raw_before1995_md = f_earthquakes_raw[(f_earthquakes_raw["time"] < f'{year_to_filter}-01-01T00:00:00.000Z') & filter_md_cond]

f_earthquakes_raw_after1995_ml = f_earthquakes_raw[(f_earthquakes_raw["time"] > f'{year_to_filter}-01-01T00:00:00.000Z') & filter_ml_cond]
f_earthquakes_raw_before1995_ml = f_earthquakes_raw[(f_earthquakes_raw["time"] < f'{year_to_filter}-01-01T00:00:00.000Z') & filter_ml_cond]


print('  ---> Saving earthquake data into parquet \n')
f_earthquakes_raw_after1995_mlmd.to_parquet('../data/extraction_layer/f_earthquakes_after1995_mlmd.parquet', engine='fastparquet')
f_earthquakes_raw_before1995_mlmd.to_parquet('../data/extraction_layer/f_earthquakes_before1995_mlmd.parquet', engine='fastparquet')

f_earthquakes_raw_after1995_md.to_parquet('../data/extraction_layer/f_earthquakes_after1995_md.parquet', engine='fastparquet')
f_earthquakes_raw_before1995_md.to_parquet('../data/extraction_layer/f_earthquakes_before1995_md.parquet', engine='fastparquet')

f_earthquakes_raw_after1995_ml.to_parquet('../data/extraction_layer/f_earthquakes_after1995_ml.parquet', engine='fastparquet')
f_earthquakes_raw_before1995_ml.to_parquet('../data/extraction_layer/f_earthquakes_before1995_ml.parquet', engine='fastparquet')

f_earthquakes_raw.to_parquet('../data/extraction_layer/f_earthquakes.parquet', engine='fastparquet')
f_earthquakes_raw.to_csv('../data/extraction_layer/f_earthquakes.csv')

print('End Data Extraction \n')
