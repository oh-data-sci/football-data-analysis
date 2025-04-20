from bs4 import BeautifulSoup
import pandas as pd
import requests

DATA_PATH_CONFIG = '../data/config/'
DATA_PATH_SOURCES = '../data/source_urls/'
URL_FILEPATH = DATA_PATH_CONFIG+'football_data_source_pages.txt'

country_urls = []
countries = []

with open(URL_FILEPATH, "r") as uf:
   country_urls = uf.read().splitlines()
   
for url in country_urls:
    country = url.split('/')[-1].split('.')[0].lower()
    if country.endswith('m'):
        country=country[:-1]
    countries.append(country)

pd.DataFrame(\
    {'country':countries,
    'url':country_urls})  \
    .to_csv(data_path+'country_url.csv',index=False)

for url in country_urls:
    country = url.split('/')[-1].split('.')[0].lower()
    if country.endswith('m'):
        country=country[:-1]
    r = requests.get(url)
    soup_page = BeautifulSoup(r.content, 'html.parser')
    table = soup_page.html.body.find_all('table')[4]
    links = table.find_all('a')

    data_urls = []
    for link in links:
        url = link.get('href')
        if url.endswith('.csv'):
            data_urls.append('https://www.football-data.co.uk/'+url)
    data_urls = list(set(data_urls)) # deduplicated
    num_urls = len(data_urls)
    df_url = pd.DataFrame({'country':[country]*num_urls, 'data_url':data_urls})
    df_url.to_csv(DATA_PATH + country + '_data_file_urls.csv', index=False)
    print(country, ':  parsed', num_urls, 'data urls')
