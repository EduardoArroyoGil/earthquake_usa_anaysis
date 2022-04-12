import pandas as pd
import numpy as np
import time

def get_year(time):
    time = str(time)
    year = time[0:4]
    return year


def generate_csv():
    print('Start generate_csv.py')
    total_start = time.process_time()

    print('     --> Start reading consolidated_data.csv...')
    start = time.process_time()
    df = pd.read_csv("../../data/consolidated_data.csv")
    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish reading consolidated_data.csv (duration: {duration} min) -->\n')

    # choosing earthquakes and the metric 'ml' wich means

    # The original magnitude relationship defined by Richter and Gutenberg in 1935 for local earthquakes.
    # It is based on the maximum amplitude of a seismogram recorded on a Wood-Anderson torsion seismograph.
    # Although these instruments are no longer widely in use, ML values are calculated using modern
    # instrumentation with appropriate adjustments. Reported by NEIC for all earthquakes in the US and Canada.
    # Only authoritative for smaller events, typically M<4.0 for which there is no mb or moment magnitude.
    # In the central and eastern United States, NEIC also computes ML, but restricts the distance range to 0-150 km.
    # In that area it is only authoritative if there is no mb_Lg as well as no mb or moment magnitude.

    print('     --> Start transformation...')
    start = time.process_time()
    df_data = df[(df.type == 'earthquake') & (df.magType == 'ml')]
    df_data = df_data.drop(columns=['Unnamed: 0', 'depth',
                                    'magType', 'nst', 'gap', 'dmin', 'rms', 'net', 'id', 'updated',
                                    'type', 'horizontalError', 'depthError', 'magError', 'magNst', 'status',
                                    'locationSource', 'magSource', 'place'])

    df_data['year'] = df_data['time'].apply(get_year)
    df_data = df_data.drop(columns=['time'])
    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish transformation (duration: {duration} min) -->\n')

    print('     --> Start saving 01.earthquakes_clean_data.csv...')
    start = time.process_time()
    df_data.to_csv("../data/01.earthquakes_clean_data.csv")
    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish saving 01.earthquakes_clean_data.csv (duration: {duration} min) -->\n')

    duration = round((time.process_time() - total_start)/60, 2)
    print(f'Finish generate_csv.py (total duration: {duration} min)\n')
