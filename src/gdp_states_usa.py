import pandas as pd
import numpy as np
from geopy import geocoders
from geopy.geocoders import Nominatim
import wbgapi as wb
from pandas_profiling import ProfileReport
from fredapi import Fred
import geopy
from geopy.geocoders import Nominatim
import time
import os
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path("../src/.env")
load_dotenv(dotenv_path=dotenv_path)

def naming(code):
    '''Function that return the name of the code state'''

    states_name_code = {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado',
        'CT': 'Connecticut', 'DE': 'Delaware',
        'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana',
        'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
        'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota',
        'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
        'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina',
        'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania',
        'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas',
        'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
        'WI': 'Wisconsin', 'WY': 'Wyoming'
    }

    name = states_name_code[code]
    return name


def get_lat(city):
    '''Function that calculates the latitude of a state'''

    geolocator = Nominatim(user_agent="data-analyst-ironhack")
    country = "United States"
    loc = geolocator.geocode(city + ',' + country)
    return loc.latitude


def get_lon(city):
    '''Function that calculates the longitude of a state'''

    geolocator = Nominatim(user_agent="data-analyst-ironhack")
    country = "United States"
    loc = geolocator.geocode(city + ',' + country)
    return loc.longitude


def get_year(time):
    '''Function that calculates the year of a date'''

    time = str(time)
    year = time[0:4]
    return year


def get_coordinates(states_name_code):
    '''Function that creates a dictionary with the coordinates for each state'''

    coordinates = {}
    for i in states_name_code.values():
        my_value = {str(i): {'lon': get_lon(i), 'lat': get_lat(i)}}
        coordinates.update(my_value)
    return coordinates


def generate_gdp_state():
    '''Function that generates gdp data and save it into a csv file'''

    # import pandas as pd
    # import numpy as np
    # from geopy import geocoders
    # from geopy.geocoders import Nominatim
    # import wbgapi as wb
    # from pandas_profiling import ProfileReport
    # from fredapi import Fred
    # import geopy
    # from geopy.geocoders import Nominatim
    print('Start generate_gdp_state.py')
    total_start = time.process_time()

    # red apy key
    print('     --> Start setting API key...')
    start = time.process_time()
    token = os.getenv("FRED_APIKEY")
    fred = Fred(api_key=token)
    duration = round((time.process_time() - start)/60,2)
    print(f'     ...Finish setting API key (duration: {duration} min) -->\n')

    # states code for get the metric of GDP for each state
    states_code = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE',
                   'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA',
                   'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV',
                   'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA',
                   'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV',
                   'WI', 'WY']

    # name of the GDP metric in fred api
    metric = 'NGSP'

    # dictionary with the state name for each code state
    states_name_code = {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado',
        'CT': 'Connecticut', 'DE': 'Delaware',
        'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana',
        'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
        'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota',
        'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
        'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina',
        'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania',
        'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas',
        'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
        'WI': 'Wisconsin', 'WY': 'Wyoming'
    }

    # calculates the coordinates for each state
    print('     --> Start getting coordinates...')
    start = time.process_time()
    states_name_coord = get_coordinates(states_name_code)
    duration = round((time.process_time() - start)/60,2)
    print(f'     ...Finish getting coordinates (duration: {duration} min) -->\n')

    # calculates the dataframe with the gdp data for each state by date
    print('     --> Start calculate dataframe with gdp data...')
    start = time.process_time()

    data = pd.DataFrame()
    for i in states_code:
        data_st = pd.DataFrame()
        data_st = pd.DataFrame(fred.get_series_latest_release(i + metric))
        data_st = data_st.reset_index()
        data_st = data_st.rename(columns={"index": "year", 0: "gdp"})
        data_st['state_code'] = i
        data = pd.concat([data, data_st])

    # calculating state name field
    data['state_name'] = data['state_code'].apply(naming)
    # calculating longitude
    data['longitude'] = data.apply(lambda my_data: states_name_coord[my_data['state_name']]['lon'], axis=1)
    # calculating latitude
    data['latitude'] = data.apply(lambda my_data: states_name_coord[my_data['state_name']]['lat'], axis=1)
    # calculating year
    data['year'] = data['year'].apply(get_year)

    duration = round((time.process_time() - start)/60,2)
    print(f'     ...Finish calculate dataframe with gdp data (duration: {duration} min) -->\n')

    # saving data into csv file
    print('     --> Start saving 02.fred_gdp_usa.csv...')
    start = time.process_time()
    data.to_csv("../data/02.fred_gdp_usa.csv")
    duration = round((time.process_time() - start)/60,2)
    print(f'     ...Finish saving 02.fred_gdp_usa.csv (duration: {duration} min) -->\n')

    duration = round((time.process_time() - total_start) / 60, 2)
    print(f'Finish generate_gdp_state.py (total duration: {duration} min)\n')
