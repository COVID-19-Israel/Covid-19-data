import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import os

OUTPUT_DIR = os.path.join('..','..','data','other',
    'israeli_health_ministry_telegram_data', 'csv', 'daily_update_new')

SOURCE_URL = "https://govextra.gov.il/ministry-of-health/corona/corona-virus/"
CSV_HEADERS = ['Mild','Moderate','Deceased','Critical','Recovered','Confirmed']
CSV_SUFFIX = '.csv'
INPUT_DATE_FORMAT = 'תמונת מצב נכונה ל- %d.%m.%Y בשעה %H:%M'
OUTPUT_DATE_FORMAT = '%Y-%m-%d_%H%M'

def format_int(value):
    try:
        return int(value)
    except:
        return int(value.strip().replace(',', ''))

def main():
    response = requests.get(SOURCE_URL)
    soup = BeautifulSoup(response.content, 'html.parser')

    confirmed = format_int(soup.find("div", {"class": "corona-sickmiddle"}).text)

    middle_containers = soup.findAll("div", {"class": "corona-deadcontainer"})
    deaths_container = middle_containers[0]
    deaths = format_int(deaths_container.find("div", {"class": "corona-lg"}).text)
    recovered_container = middle_containers[1]
    recovered = format_int(recovered_container.find("div", {"class": "corona-lg"}).text)

    levels_container = soup.find("div", {"class": "corona-sickfooter"})
    mild = format_int(levels_container.findAll("div", {"class": "corona-bold"})[0].text)
    moderate = format_int(levels_container.findAll("div", {"class": "corona-bold"})[1].text)
    critical = format_int(levels_container.findAll("div", {"class": "corona-bold"})[2].text)

    update_time_div = soup.find(text=re.compile('תמונת מצב נכונה ל'))
    update_time = datetime.strptime(update_time_div, INPUT_DATE_FORMAT)\
        .strftime(OUTPUT_DATE_FORMAT)

    csv_values = [mild,moderate,deaths,critical,recovered,confirmed]
    csv_table = pd.DataFrame(columns=CSV_HEADERS, data=[csv_values])
    file_path = os.path.join(OUTPUT_DIR,update_time) + CSV_SUFFIX

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_table.to_csv(file_path, index=False, encoding="utf-8")

if __name__ == "__main__":
    main()
