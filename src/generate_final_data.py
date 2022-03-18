import pandas as pd
import geopy.distance
from math import sin, cos, sqrt, atan2, radians
import time


def calc_distance(x_cent, y_cent, x_obj, y_obj):
    '''This fucntion calculates the distance between two points in 2D
    '''
    x_diff = x_cent - x_obj
    y_diff = y_cent - y_obj

    distance_sqrt = x_diff ** 2 + y_diff ** 2
    distance = distance_sqrt ** (0.5)

    return distance


def calc_distance_geopy(x_cent, y_cent, x_obj, y_obj):
    '''This fucntion calculates the distance between two points using geopy
    '''
    coords_1 = (x_cent, y_cent)
    coords_2 = (x_obj, y_obj)
    return geopy.distance.vincenty(coords_1, coords_2).km


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


def generate_final_data():
    '''Generates the final data to analyse with the earthquake and gdp data merged in same dataframe'''

    print('Start generate_final_data.py')
    total_start = time.process_time()

    # importing csvs for earthquakes and gdp data
    print('     --> Start importing csvs for earthquakes and gdp data...')
    start = time.process_time()
    df_eq = pd.read_csv("../data/01.earthquakes_clean_data.csv").drop(columns=['Unnamed: 0'])
    df_gdp = pd.read_csv("../data/02.fred_gdp_usa.csv").drop(columns=['Unnamed: 0'])
    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish importing csvs for earthquakes and gdp data (duration: {duration} min) -->\n')

    # renaming columns for both csvs
    print('     --> Start renaming columns for both csvs...')
    start = time.process_time()
    df_eq = df_eq.rename(columns={"latitude": "latitude_eq", "longitude": "longitude_eq"})
    df_gdp = df_gdp.rename(columns={"latitude": "latitude_gdp", "longitude": "longitude_gdp"})
    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish renaming columns for both csvs (duration: {duration} min) -->\n')

    # merging both data frames: earthquakes and gdps
    print('     --> Start merging both data frames: earthquakes and gdps...')
    start = time.process_time()
    df_merged = pd.merge(df_eq, df_gdp, how="inner", left_on='year', right_on='year')
    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish merging both data frames: earthquakes and gdps (duration: {duration} min) -->\n')

    # calculation of the distance between the coordinates of each states and each seismic event
    print('     --> Start calculation of the distance...')
    start = time.process_time()
    df_merged['distance'] = df_merged.apply(
        lambda my_data: calc_distance_math(my_data['longitude_gdp'], my_data['latitude_gdp'], my_data['longitude_eq'],
                                           my_data['latitude_eq']), axis=1)
    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish calculation of the distance (duration: {duration} min) -->\n')

    # filtering just the events that has a distance less than 600 km
    print('     --> Start filtering just the events that has a distance less than 600 km...')
    start = time.process_time()
    df_data = df_merged[(df_merged.distance <= 600)]
    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish filtering just the events that has a distance less than 600 km (duration: {duration} min) -->\n')

    # saving data into a csv file
    print('     --> Start saving 03.df_data_mag_gdp.csv...')
    start = time.process_time()
    df_data.to_csv("../data/03.df_data_mag_gdp.csv")
    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish saving 03.df_data_mag_gdp.csv (duration: {duration} min) -->\n')

    duration = round((time.process_time() - total_start) / 60, 2)
    print(f'Finish generate_final_data.py (total duration: {duration} min)')
