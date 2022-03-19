import pandas as pd
import numpy as np
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import pickle

def get_df_state(state, df):
    '''This function create a dataframe for the state indicated with the metrics grouped for each year'''

    df_state = df[(df.state_name == state)].drop(columns=['Unnamed: 0', 'latitude_eq',
                                                          'longitude_eq', 'longitude_gdp',
                                                          'latitude_gdp', 'distance'])

    df_state_gdp = df_state.groupby(by=["state_code", "state_name", "year"], dropna=False).mean().drop(
        columns='mag').reset_index()
    df_state_mag = df_state.groupby(by=["state_code", "state_name", "year"], dropna=False).max().drop(
        columns='gdp').reset_index()

    df_state = pd.merge(df_state_gdp, df_state_mag,
                        how="inner",
                        left_on=['year', 'state_code', 'state_name'],
                        right_on=['year', 'state_code', 'state_name'])
    return df_state


def get_df_usa(df):
    '''This function create a dataframe for the state indicated with the metrics grouped for each year'''

    df_usa = df.drop(columns=['Unnamed: 0', 'latitude_eq',
                              'longitude_eq', 'longitude_gdp',
                              'latitude_gdp', 'distance'])

    df_usa_gdp = df_usa.groupby(by=["year"], dropna=False).mean().drop(columns='mag').reset_index()
    df_usa_mag = df_usa.groupby(by=["year"], dropna=False).max().drop(columns='gdp').reset_index()

    df_usa = pd.merge(df_usa_gdp, df_usa_mag,
                      how="inner",
                      left_on=['year'],
                      right_on=['year'])
    return df_usa


def get_df_growth_state(state, df):
    '''This function generates a dataframe with the growth of gdp for a state'''
    df_state = get_df_state(state, df)
    df_state_past = df_state.copy()

    df_state_past['year'] = df_state_past['year'].apply(int)
    df_state_past['year'] = df_state_past['year'] + 1

    df_state_past = df_state_past.rename(columns={"state_code": "state_code_past",
                                                  "state_name": "state_name_past",
                                                  "year": "year_past",
                                                  "gdp": "gdp_past",
                                                  "mag": "mag_past"})

    df_state_growth = pd.merge(df_state, df_state_past,
                               how="left",
                               left_on=['year', 'state_code', 'state_name'],
                               right_on=['year_past', 'state_code_past', 'state_name_past']).drop(
        columns=['state_code_past', 'state_name_past', 'year_past', 'mag_past'])

    df_state_growth['growth_gdp'] = df_state_growth['gdp'] - df_state_growth['gdp_past']
    df_state_growth = df_state_growth.drop(columns='gdp_past')

    return df_state_growth


def get_df_growth_usa(df):
    '''This function generates a dataframe with the growth of gdp for a state'''
    df_usa = get_df_usa(df)
    df_usa_past = df_usa.copy()

    df_usa_past['year'] = df_usa_past['year'].apply(int)
    df_usa_past['year'] = df_usa_past['year'] + 1

    df_usa_past = df_usa_past.rename(columns={
        "year": "year_past",
        "gdp": "gdp_past",
        "mag": "mag_past"})

    df_usa_growth = pd.merge(df_usa, df_usa_past,
                             how="left",
                             left_on=['year'],
                             right_on=['year_past']).drop(
        columns=['year_past', 'mag_past'])

    df_usa_growth['growth_gdp'] = df_usa_growth['gdp'] - df_usa_growth['gdp_past']
    df_usa_growth = df_usa_growth.drop(columns='gdp_past')

    return df_usa_growth


def create_double_yaxis(df, state, gdp_measure='gdp'):
    '''This function create a linechart with double Y axis '''

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Scatter(x=list(df['year']), y=list(df['mag']), name="mag"),
        secondary_y=False,
    )
    if gdp_measure == 'gdp':
        fig.add_trace(
            go.Scatter(x=list(df['year']), y=list(df['gdp']), name="gdp"),
            secondary_y=True,
        )
    elif gdp_measure == 'growth_gdp':
        fig.add_trace(
            go.Scatter(x=list(df['year']), y=list(df['growth_gdp']), name="growth gdp"),
            secondary_y=True,
        )

    # Add figure title
    if gdp_measure == 'gdp':
        fig.update_layout(
            title_text=state.lower().capitalize() + " GDP vs Sismic Magnitude Correlation"
        )
    elif gdp_measure == 'growth_gdp':
        fig.update_layout(
            title_text=state.lower().capitalize() + " Growth GDP vs Sismic Magnitude Correlation"
        )

    # Set x-axis title
    fig.update_xaxes(title_text="Year")

    # Set y-axes titles
    fig.update_yaxes(title_text="<b>mag</b> sismic magnitude", secondary_y=False)

    if gdp_measure == 'gdp':
        fig.update_yaxes(title_text="<b>gdb</b> gross domestic product", secondary_y=True)
    elif gdp_measure == 'growth_gdp':
        fig.update_yaxes(title_text="<b>growth gdb</b> gross domestic product", secondary_y=True)

    return fig


def analysis():
    '''Function to launch the whole analysis for earthquakes and gbp'''

    print('Start analysis.py')
    total_start = time.process_time()

    #  Reading the final processed data
    print('     --> Start Reading the final processed data...')
    start = time.process_time()
    df = pd.read_csv("../data/03.df_data_mag_gdp.csv")

    states = list(df['state_name'].unique())
    graphs = dict()
    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish Reading the final processed data (duration: {duration} min) -->\n')

    #  generation of graphs for each state of USA
    print('     --> Start generation of graphs for each state of USA...')
    start = time.process_time()
    for i in states:
        df_state = get_df_growth_state(i, df)
        fig_gdp = create_double_yaxis(df_state, i)
        fig_growth_gdp = create_double_yaxis(df_state, i, 'growth_gdp')

        name_gdp = i + '_gdp-vs-mag.png'
        name_growth_gdp = i + '_growth_gdp-vs-mag.png'

        fig_gdp.write_image("../images/" + name_gdp)
        fig_growth_gdp.write_image("../images/" + name_growth_gdp)

        graphs.update({i: {'gdp': fig_gdp, 'growth_gdp': fig_growth_gdp}})

    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish generation of graphs for each state of USA (duration: {duration} min) -->\n')

    #  generation of graphs for the whole USA
    print('     --> Start generation of graphs for the whole USA...')
    start = time.process_time()

    df_usa = get_df_growth_usa(df)
    fig_gdp = create_double_yaxis(df_usa, 'USA')
    fig_growth_gdp = create_double_yaxis(df_usa, 'USA', 'growth_gdp')
    graphs.update({'USA': {'gdp': fig_gdp, 'growth_gdp': fig_growth_gdp}})

    graphs['USA']['gdp'].write_image("../images/usa_gdp-vs-mag.png")
    graphs['USA']['growth_gdp'].write_image("../images/usa_growth_gdp-vs-mag.png")

    duration = round((time.process_time() - start)/60, 2)
    print(f'     ...Finish generation of graphs for the whole USA (duration: {duration} min) -->\n')

    # open file for writing
    f = open("../data/graphs.pkl", "wb")

    # write the python object (dict) to pickle file
    pickle.dump(graphs, f)

    # close file
    f.close()

    duration = round((time.process_time() - total_start) / 60, 2)
    print(f'Finish analysis.py (total duration: {duration} min)')
