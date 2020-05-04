import requests
from bs4 import BeautifulSoup
import os, sys, time ,datetime

def fileCreation(path,data):
    try:
        open(path, 'wb').write(data)
        return True
    except OSError:
        return False

def scrape(url,outputPath):
    countryFile = requests.get(url)
    print(countryFile.status_code)
    fileName = outputPath + f"/mobilityReport_{date}.csv"
    print(f"{date} file created") if fileCreation(fileName,countryFile.content) else sys.exit(f"Could not create {countryName} file")

date = datetime.datetime.now()
date = date.strftime("%d"+"_"+"%m"+"_"+"%y")
outputPath = f'../../data/other/googleMobilityScraper/csv'

url ='https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'

scrape(url,outputPath)