import requests
from bs4 import BeautifulSoup
import time
import datetime
import os

url ='https://www.worldometers.info/coronavirus//'
headers= {'User-Agent': 'Mozilla/5.0'}
response = requests.get(url)
print(response.status_code)

soup = BeautifulSoup(response.content, 'html.parser')
country_table = soup.find(id='main_table_countries_today')

date = datetime.datetime.now()
date = date.strftime("%d"+"_"+"%m"+"_"+"%y")
filename = f'../../data/other/worldmeterScraper/csv/worldmeter_table_{date}.csv'

with open(filename, 'w') as r:
    for row in country_table.find_all('thead'):
        for cell in row.find_all('th'):
            if cell.text != 'Country,Other':
                r.write('"'+cell.text+'"'+',')
            else:
                r.write('Country,')
        r.write('\n')
    for row in country_table.find_all('tr'):
        for cell in row.find_all('td'):
            try:
                r.write('"'+cell.text+'"'+',')
            except UnicodeEncodeError as e:
                r.write('???,')
        r.write("\n")