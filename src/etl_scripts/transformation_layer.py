import pandas as pd
import uuid
from math import sin, cos, sqrt, atan2, radians
import dask.dataframe as dd

def get_year(time):
    time = str(time)
    year = time[0:4]
    return int(year)

def calc_distance_math(lon_cent ,lat_cent ,lon_obj ,lat_obj):
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

    a = sin(dlat / 2 )**2 + cos(lat1) * cos(lat2) * sin(dlon / 2 )**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance

def generate_final_data(df_eq ,df_gdp):

    '''Generates the final data to analyse with the earthquake and gdp data merged in same dataframe'''

    # importing csvs for earthquakes and gdp data
    df_eq = df_eq.drop(columns=['Unnamed: 0'])
    df_gdp = df_gdp.drop(columns=['Unnamed: 0'])

    # renaming columns for both csvs
    df_eq = df_eq.rename(columns={"latitude": "latitude_eq", "longitude": "longitude_eq"})
    df_gdp = df_gdp.rename(columns={"latitude": "latitude_gdp", "longitude": "longitude_gdp"})

    # creating column year for df of earthquakes
    df_eq['year'] = df_eq['time'].apply(get_year)

    # creating dask dataframes
    ddf_eq = dd.from_pandas(df_eq, npartitions=6).set_index('year')
    ddf_gdp = dd.from_pandas(df_gdp, npartitions=6).set_index('year')

    # merging both data frames: earthquakes and gdps
    ddf_merged = dd.merge(ddf_eq, ddf_gdp,
                          how="inner",
                          left_index=True, right_index=True).compute()

    # calculation of the distance between the coordinates of each states and each seismic event
    ddf_merged['distance' ] =ddf_merged.apply \
        (lambda my_data: calc_distance_math(my_data['longitude_gdp'] ,my_data['latitude_gdp'] ,my_data['longitude_eq']
                                           ,my_data['latitude_eq']), axis = 1)

    # filtering just the events that has a distance less than 600 km
    ddf_data = ddf_merged[(ddf_merged.distance <= 600) ]

    return ddf_data

print('Ingesting data')
# reading the main tables of the facts that will be inserted in the data base as data model
f_earthquakes_raw = pd.read_csv("../data/consolidated_data.csv")
f_gdp_raw = pd.read_csv("../data/02.fred_gdp_usa.csv")

print('Setting ids')
# calculating the id for each fact table (earthquakes and gdp) based on uuid (hash function)
f_earthquakes_raw['id_earthquake']=f_earthquakes_raw.apply(lambda my_data: str(my_data['time'])+str(my_data['latitude'])+str(my_data['longitude']), axis = 1)
f_earthquakes_raw['id_earthquake']=f_earthquakes_raw.apply(lambda my_data: str(uuid.uuid5(uuid.NAMESPACE_DNS, my_data['id_earthquake'])), axis = 1)

f_gdp_raw['id_gdp']=f_gdp_raw.apply(lambda my_data: str(my_data['year'])+str(my_data['latitude'])+str(my_data['longitude']), axis = 1)
f_gdp_raw['id_gdp']=f_gdp_raw.apply(lambda my_data: str(uuid.uuid5(uuid.NAMESPACE_DNS, my_data['id_gdp'])), axis = 1)

print('Creating linktable')
#creating the link table between two tables
link_earthquake_gdp_raw = generate_final_data(f_earthquakes_raw,f_gdp_raw)

# saving it
link_earthquake_gdp_raw = link_earthquake_gdp_raw.sample(10)
print(link_earthquake_gdp_raw)
link_earthquake_gdp_raw.to_csv("../data/link_earthquake_gdp_raw.csv")