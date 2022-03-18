import pandas as pd
import geopy.distance
from math import sin, cos, sqrt, atan2, radians


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

    print('Finish generate_final_data.py')

    # importing csvs for earthquakes and gdp data
    print('     --> Start importing csvs for earthquakes and gdp data')
    df_eq = pd.read_csv("../data/01.earthquakes_clean_data.csv").drop(columns=['Unnamed: 0'])
    df_gdp = pd.read_csv("../data/02.fred_gdp_usa.csv").drop(columns=['Unnamed: 0'])
    print('     Finish importing csvs for earthquakes and gdp data -->\n')

    # renaming columns for both csvs
    print('     --> Start renaming columns for both csvs')
    df_eq = df_eq.rename(columns={"latitude": "latitude_eq", "longitude": "longitude_eq"})
    df_gdp = df_gdp.rename(columns={"latitude": "latitude_gdp", "longitude": "longitude_gdp"})
    print('     Finish renaming columns for both csvs -->\n')

    # merging both data frames: earthquakes and gdps
    print('     --> Start merging both data frames: earthquakes and gdps')
    df_merged = pd.merge(df_eq, df_gdp, how="inner", left_on='year', right_on='year')
    print('     Finish merging both data frames: earthquakes and gdps -->\n')

    # calculation of the distance between the coordinates of each states and each seismic event
    print('     --> Start calculation of the distance')
    df_merged['distance'] = df_merged.apply(
        lambda my_data: calc_distance_math(my_data['longitude_gdp'], my_data['latitude_gdp'], my_data['longitude_eq'],
                                           my_data['latitude_eq']), axis=1)
    print('     Finish calculation of the distance -->\n')

    # filtering just the events that has a distance less than 600 km
    print('     --> Start filtering just the events that has a distance less than 600 km')
    df_data = df_merged[(df_merged.distance <= 600)]
    print('     Finish filtering just the events that has a distance less than 600 km -->\n')

    # saving data into a csv file
    print('     --> Start saving 03.df_data_mag_gdp.csv')
    df_data.to_csv("../data/03.df_data_mag_gdp.csv")
    print('     Finish saving 03.df_data_mag_gdp.csv -->\n')

    print('Finish generate_final_data.py')

generate_final_data()