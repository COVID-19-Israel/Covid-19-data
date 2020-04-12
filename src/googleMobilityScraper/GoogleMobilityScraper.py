import requests
from bs4 import BeautifulSoup
import os, sys, time ,datetime

def dirCreation (dirName):
    try:
        os.mkdir(dirName)
        return True
    except OSError:
        return False

def fileCreation(path,data):
    try:
        open(path, 'wb').write(data)
        return True
    except OSError:
        return False

def scrape(html,outputPath):
    country_table = html.find(id='glue-filter-result-container')
    for row in country_table.find_all('div', class_ = 'glue-expansion-panel glue-filter-result__item glue-filter-is-matching'):
        for countryNameHeader in row.find_all('h1', class_ = 'glue-headline glue-headline--headline-6 country-name' ):
            countryName = countryNameHeader.text.replace("\n","").strip().replace(" ","_")
        for urlData in row.find_all('a', href=True ):
            url = urlData['href']
        countryFile = requests.get(url)
        fileName = outputPath + f"/{countryName}_{date}.pdf"
        print(f"{countryName} file created") if fileCreation(fileName,countryFile.content) else sys.exit(f"Could not create {countryName} file")
        #Check if there is a lower regional level
        for subRow in row.find_all('div', class_ = 'region-row glue-filter-result__item glue-filter-is-matching'):
            for regionNameHeader in subRow.find_all('h1', class_='glue-headline glue-headline--headline-6 region-name'):
                regionName = regionNameHeader.text.replace(" ", "").replace("\n", "")
            for urlData in subRow.find_all('a', href=True):
                url = urlData['href']
            countryFile = requests.get(url)
            fileName = f"./{date}/{countryName}_{regionName}_{date}.pdf"
            print(f"{countryName}, {regionName} file created") if fileCreation(fileName, countryFile.content) else sys.exit(
                f"Could not create {countryName} file")


date = datetime.datetime.now()
date = date.strftime("%d"+"_"+"%m"+"_"+"%y")
dirName = f'../../data/other/googleMobilityScraper/pdf/{date}'
print("Dir created") if dirCreation(dirName) else sys.exit("Could not create dir")

url ='https://www.google.com/covid19/mobility/'
headers= {'User-Agent': 'Mozilla/5.0'}

response = requests.get(url)
print(response.status_code)
html = BeautifulSoup(response.content, 'html.parser')
scrape(html,dirName)
