#!/usr/bin/env python
# coding: utf-8

# In[22]:


import requests
from bs4 import BeautifulSoup
import time
import datetime


# In[17]:


url ='https://www.worldometers.info/coronavirus//'
headers= {'User-Agent': 'Mozilla/5.0'}


# In[18]:


response = requests.get(url)
print(response.status_code)


# In[19]:


soup = BeautifulSoup(response.content, 'html.parser')


# In[55]:


country_table = soup.find(id='main_table_countries_today')


# In[32]:


date = datetime.datetime.now()
date = date.strftime("%d"+"_"+"%m"+"_"+"%y")
filename = f"worldmeter_table_{date}.csv"


# In[54]:


with open (filename,'w') as r:
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


# In[ ]:




