# COVID-19-Israel Data Repository

This is a data repository containing information about the Covid19 Coronavirus. 
Our goal is to gather as much information from different sources to help analysts and researchers. 
We update our data daily.

## How to use our data

It is possible to download all of our information from our website (http://covidil.hopto.org/dashboard), as csv files. Because we gather information from different data sources - it may be inconsistent. We don't remove or solve these inconsistencies, we leave this job to the analysts to solve according to their understanding. A few recommendations:
•	Use the specific data base if they exist - for example, if you want to find info about Italy, use COVID-19 Italia.
•	Notice that some data appears more than once per each date - you can filter this using the update_time column.
•	In some countries we have a division into smaller regions. When summing info about that country, be sure to notice not to sum the different regions with a row that already includes the sum of the whole country.


## Table's and Main column's

•	Reports - Contains most of the information of the DB, divided by geographical area and date.
•	Lockdown - Contains information about social distancing steps taken in different countries to fight the virus. See more documentation in Lockdown_directory.


### Reports

Column Name | Column Meaning
------------ | -------------
country, state, county, city | Geographical area, in different resolutions. Not all resolutions exist for every country.
Population | The sum of total population.
update_time | The time the original DB we took the data from was updated.
db_source_name | The name of the source DB.
db_source_url | The URL of the original DB.
db_source_time | The time we updated out DB.
confirmed | Total number of confirmed cases up to this date.
deaths | Total number of deceased up to this date.
recovered | Total number of recovered up to this date.
tested | Total number of tested up to this date.

### Lockdown

Column Name | Column Meaning
------------ | -------------
country, state, county, city | Geographical area, in different resolutions.
Population | The sum of total population.
start_date | Date where the lockdown step was taken.
lockdown_level | An integer indicating the severity of the social distancing. See Lockdown_directory for more details.
Lockdown parameters | Social distancing steps and a true/false indicator if it was taken or not. 

## Data Sources

Tabular data:
* Johns Hopkins CSSE: https://github.com/CSSEGISandData/COVID-19
* covid19-eu-data: https://github.com/covid19-eu-zh/covid19-eu-data
* COVID-19 Italia - Monitoraggio situazione: https://github.com/pcm-dpc/COVID-19
* The COVID Tracking Project (relevant to USA): https://covidtracking.com/
* COVID19 Spain cases: https://github.com/victorvicpal/COVID19_es
* COVID-19 South Korea: https://github.com/parksw3/COVID19-Korea

Additional data:
* Lockdown status - see Lockdown directory
* World population - see Other_data directory
* Total number of tests - see Other_data directory

## Website
http://covidil.hopto.org/dashboard

## Contact us:
Found problems with the data? Have some good ideas and want to help?
You can contact us at: iddo.waxman@gmail.com
