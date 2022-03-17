import pandas as pd
from fredapi import Fred

def naming(code):
        states_name_code = {
            'AL':'Alabama','AK':'Alaska','AZ':'Arizona','AR':'Arkansas','CA':'California','CO':'Colorado','CT':'Connecticut','DE':'Delaware',
            'FL':'Florida','GA':'Georgia','HI':'Hawaii','ID':'Idaho','IL':'Illinois','IN':'Indiana','IA':'Iowa','KS':'Kansas','KY':'Kentucky','LA':'Louisiana',
            'ME':'Maine','MD':'Maryland','MA':'Massachusetts','MI':'Michigan','MN':'Minnesota','MS':'Mississippi','MO':'Missouri','MT':'Montana','NE':'Nebraska','NV':'Nevada',
            'NH':'New Hampshire','NJ':'New Jersey','NM':'New Mexico','NY':'New York','NC':'North Carolina','ND':'North Dakota','OH':'Ohio','OK':'Oklahoma','OR':'Oregon','PA':'Pennsylvania',
            'RI':'Rhode Island','SC':'South Carolina','SD':'South Dakota','TN':'Tennessee','TX':'Texas','UT':'Utah','VT':'Vermont','VA':'Virginia','WA':'Washington','WV':'West Virginia',
            'WI':'Wisconsin','WY':'Wyoming'
        }

        name = states_name_code[code]
        return name

def generate_data(states_code, metric):

    fred = Fred(api_key='70e6406526da042e6e900cae78b217e1')
    data = pd.DataFrame()
    for i in states_code:
        data_st = pd.DataFrame()
        data_st = pd.DataFrame(fred.get_series_latest_release(i+metric))
        data_st = data_st.reset_index()
        data_st = data_st.rename(columns={"index": "Year", 0: "Value"})
        data_st['State Code'] = i
        data = pd.concat([data,data_st])

    return data

def create_name_state_column(data):
    data['State Name']=data['State Code'].apply(naming)
    return data

def gdp_states_usa():

    states_code = ['AL','AK','AZ','AR','CA','CO','CT','DE',
    'FL','GA','HI','ID','IL','IN','IA','KS','KY','LA',
    'ME','MD','MA','MI','MN','MS','MO','MT','NE','NV',
    'NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA',
    'RI','SC','SD','TN','TX','UT','VT','VA','WA','WV',
    'WI','WY']

    metric = 'NGSP'

    print('starting genereation data from fred by year and state in USA')
    data = generate_data(states_code, metric)
    print('finishing genereation data from fred by year and state in USA')


    print('creating new sate name column')
    data = create_name_state_column(data)