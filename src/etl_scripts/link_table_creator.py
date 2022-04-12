import pandas as pd
from math import sin, cos, sqrt, atan2, radians
from tqdm import tqdm, tqdm_pandas


def calc_distance_math(lon_cent, lat_cent, lon_obj, lat_obj):
    '''
    '''
    # approximate radius of earth in km
    R = 6373.0

    lat1 = radians(lat_cent)
    lon1 = radians(lon_cent)
    lat2 = radians(lat_obj)
    lon2 = radians(lon_obj)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance


print('LINK TABLE CREATOR ------------ \n', 'Start creator of the link table between earthquakes and gdp data')
print('  ---> Reading earthquake and gdp data from parquet \n')
f_earthquakes = pd.read_parquet('../data/extraction_layer/f_earthquakes.parquet', engine='fastparquet')
f_gdp = pd.read_parquet('../data/extraction_layer/f_gdp.parquet', engine='fastparquet')

print('  ---> Dropping not needed columns \n')
drop_eq_columns = ['id_earthquake', 'time',  'depth', 'mag', 'magType', 'nst', 'gap', 'dmin', 'rms',
                    'net', 'id', 'place', 'type', 'horizontalError', 'depthError',
                    'magError', 'magNst', 'status', 'locationSource', 'magSource',
                    'updated_date']
drop_gdp_columns = ['id_gdp', 'gdp', 'state_code',
                    'state_name']

f_earthquakes = f_earthquakes.drop(columns=drop_eq_columns)
f_gdp = f_gdp.drop(columns=drop_gdp_columns)

print('  ---> Renaming earthquake and gdp columns \n')
columns_eq_renaming = {"year":"year_eq","longitude":"longitude_eq","latitude":"latitude_eq"}
columns_gdp_renaming = {"year":"year_gdp","longitude":"longitude_gdp","latitude":"latitude_gdp"}

f_earthquakes.rename(columns=columns_eq_renaming ,inplace=True)
f_gdp.rename(columns=columns_gdp_renaming ,inplace=True)

print('  ---> Dropping duplicates \n')
f_gdp.drop_duplicates(inplace=True)
f_earthquakes.drop_duplicates(inplace=True)

print('  ---> Creating link table \n')
key_left = ['year_eq']
key_right = ['year_gdp']

df_merged = pd.merge(f_earthquakes, f_gdp,
                          how="inner",
                          left_on=key_left, right_on=key_right)

print('  ---> Calculating the distance \n')

tqdm.pandas()

df_merged['distance'] = df_merged.progress_apply(
        lambda my_data: calc_distance_math(my_data['longitude_gdp'], my_data['latitude_gdp'], my_data['longitude_eq'],
                                           my_data['latitude_eq']), axis=1)

print('\n  ---> Dropping not needed columns \n')
drop_merge_columns = ['year_eq', 'latitude_eq',  'longitude_eq', 'year_gdp', 'longitude_gdp', 'latitude_gdp']

df_merged = df_merged.drop(columns=drop_merge_columns)

print('  ---> Filtering distance less than 600 km \n')
df_data = df_merged[(df_merged.distance <= 600)]

print('  ---> Dropping distance column \n')
drop_data_columns = ['distance']
df_data = df_data.drop(columns=drop_data_columns)

print('  ---> Saving Link Table \n')
df_data.to_parquet('../data/extraction_layer/link_eq_gdp.parquet', engine='fastparquet')
df_data.to_csv('../data/extraction_layer/link_eq_gdp.csv')

print('Start creator of the link table between earthquakes and gdp data \n')