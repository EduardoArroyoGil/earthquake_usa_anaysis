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
The data is taken from [Kaggle](https://www.kaggle.com/danielpe/earthquakes) refered to the [USGS data](https://earthquake.usgs.gov/earthquakes/feed/v1.0/csv.php) and feeded by [FRED API](https://fred.stlouisfed.org/docs/api/fred/). Therefore both data will be merged by year and by state of USA.

To refresh and check the process running [click here](/notebooks/Process%20Trigger.ipynb)

##   Folder Structure:
The folder structure is based on four folders:
  * **/data**: Where is located the inicial data (*consolidated_data.csv*) of the earthquakes and that data cleaned (*01.earthquakes_clean_data.csv*), the data obtained from [FRED API](https://fred.stlouisfed.org/docs/api/fred/) (*02.fred_gdp_usa.csv*), transformed and enriched data (*03.df_data_mag_gdp.csv*) and other data generated for rest of analysis process.
  * **/images**: In this folder images needed for the project and the ones generated with graphs to explain the realtion between GDP and eartthquakes are stored for following usage.
  * **/noteboooks**: In here jupyter notbooks are saved to explain a deeper analysis and an analysis of the procedure how it has been run describing it with a log
  * **/src**: Soruce folder (*src*) is focused to store all the ".py" files that generates all the data for the analysis. The sturcutre of the code is based on the main.py file that calls the other ".py" files in order and generates the log to check if everything has been going well.
  
##   Conclusions:
In general we see ...

![](/images/usa_gdp-vs-mag.png)
![](/images/usa_growth_gdp-vs-mag.png)

For more datails you can find them [here](/notebooks/Earthquake%20Analysis.ipynb)

## Libraries:
[sys](https://docs.python.org/3/library/sys.html)
[requests](https://pypi.org/project/requests/2.7.0/)
[pandas](https://pandas.pydata.org/)
[dotenv](https://pypi.org/project/python-dotenv/)
[os](https://docs.python.org/3/library/os.html)
[re](https://docs.python.org/3/library/re.html)
