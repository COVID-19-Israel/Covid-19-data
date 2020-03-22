from lxml import etree
import csv
import pandas as pd
import urllib

OUTPUT_PATH = 'covid19_tests.csv'
TABLE_SITE_URL = "https://ourworldindata.org/coronavirus-testing-source-data"

web = urllib.urlopen(TABLE_SITE_URL)
s = web.read()

html = etree.HTML(s)

## get the table

tr_nodes = html.xpath('//table/tbody/tr')

td_content = [[td.text.encode('ascii', 'ignore') if type(td.text) in [str, unicode] else "" for td in tr.xpath('td')[:-3]] for tr in tr_nodes[1:]]


## write to csv

csv_data = pd.DataFrame(td_content)

csv_data.to_csv(OUTPUT_PATH, index=False, header=["country","number_of_test","update_date"])
