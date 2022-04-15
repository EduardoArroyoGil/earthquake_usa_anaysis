# Earthquake USA Anaysis
Analysis about how earthquakes affect to the GDP of each state of USA

![](/images/1030_SS_earthquake-1028x579.jpeg)


##   Introduction:
Earthquakes generate many disasters at all levels: at a social level, at a personal level, at an infrastructure level...
This can affect the social development of an area immediately and also in the long term.

That is why it would be interesting to measure how earthquakes affect the economy of a region, since it is a good way to see how it affects society in aggregate.

##   Objectives:
The objective of this project is to find the level of affection of an earthquake to society in a certain region for various regions of the USA. Mainly to the GDP of the study area.

##   Data:
The data is taken from [Kaggle](https://www.kaggle.com/danielpe/earthquakes) refered to the [USGS data](https://earthquake.usgs.gov/earthquakes/feed/v1.0/csv.php) (to understand better and deeper the data set click [here](notebooks/Scanning%20Earthquake%20Dataset.ipynb)) and feeded by [FRED API](https://fred.stlouisfed.org/docs/api/fred/). Therefore both data will be merged by year and by state of USA.

To refresh and check the process running [click here](/notebooks/Process%20Trigger.ipynb)

##   Folder Structure:
The folder structure is based on four folders:
  * **/data**: Where is located the inicial data (*consolidated_data.csv*) of the earthquakes and that data cleaned (*01.earthquakes_clean_data.csv*), the data obtained from [FRED API](https://fred.stlouisfed.org/docs/api/fred/) (*02.fred_gdp_usa.csv*), transformed and enriched data (*03.df_data_mag_gdp.csv*) and other data generated for rest of analysis process.
  * **/images**: In this folder images needed for the project and the ones generated with graphs to explain the realtion between GDP and eartthquakes are stored for following usage.
  * **/noteboooks**: In here jupyter notbooks are saved to explain a deeper analysis and an analysis of the procedure how it has been run describing it with a log
  * **/src**: Soruce folder (*src*) is focused to store all the ".py" files that generates all the data for the analysis. The sturcutre of the code is based on the main.py files (etl_main.py for the etl process to do an analysis using Tableau reading a Data base and analysis_main.py for the first analysis done in Jupyter Notebooks) that calls the other ".py" files in order and generates the log to check if everything has been going well.

## Interactive Dashboard:
To create an interactive analysis Tableau has been selected as the tool to deliver the best user experience. The dashboard is built as a newspaper to simulate a historic analysis:

here you can find the [Dashboard](https://public.tableau.com/app/profile/eduardo.arroyo.gil/viz/EarthquakeNews/Seismicevent)
  
##   Main Conclusions:
We can see in general that when **a peak of ML happen is follow by a stalling or decresing of growth of GDP or Net GDP**.

<img src="/images/usa_gdp-vs-mag.png" alt="drawing" width="375"/> <img src="/images/usa_growth_gdp-vs-mag.png" alt="drawing" width="375"/>

*Note: the seismic magnitude is ML which is explain in [here](notebooks/Scanning%20Earthquake%20Dataset.ipynb)*

For more datails in an analytical sense, you can find them [here](notebooks/Earthquake%20Analysis.ipynb)

## Libraries:
* [sys](https://docs.python.org/3/library/sys.html)
* [requests](https://pypi.org/project/requests/2.7.0/)
* [pandas](https://pandas.pydata.org/)
* [dotenv](https://pypi.org/project/python-dotenv/)
* [os](https://docs.python.org/3/library/os.html)
* [re](https://docs.python.org/3/library/re.html)
* [time](https://docs.python.org/3/library/time.html)
* [geopy](https://geopy.readthedocs.io/en/stable/)
* [fredapi](https://github.com/mortada/fredapi)
* [pathlib](https://docs.python.org/3/library/pathlib.html)
* [bs4](https://pypi.org/project/bs4/)
* [requests](https://pypi.org/project/requests/)
* [math](https://docs.python.org/3/library/math.html)
* [plotly](https://plotly.com/)
* [pickle](https://docs.python.org/3/library/pickle.html)
* [tqdm]()
* [sqlalchemy]()
* [uuid]()
