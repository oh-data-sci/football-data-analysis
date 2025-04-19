from bs4 import BeautifulSoup
import chardet
import datetime
import duckdb
import io
import pandas as pd
import requests

DATA_PATH = '../data/'
COUNTRY_LIST = 'config/country_url.csv'


def read_problematic_csv(url):
    # fetch file content of the linked csv file
    response = requests.get(url)
    content = response.content
    
    # detect the encoding
    detected = chardet.detect(content)
    encodings_to_try = [
        detected['encoding'],  # first attempt with detected encoding
        'windows-1252',        # maybe windows encoding?
        'latin-1',             # a permissive encoding
        'utf-8'                # standard, preferred encoding
    ]
    
    # remove duplicates, remove 'None's
    encodings_to_try = list(set([e for e in encodings_to_try if e]))
    
    # try each encoding 
    for encoding in encodings_to_try:
        try:
            csv_content = content.decode(encoding)
            # determine the number of columns from the header
            first_line = csv_content.split('\n')[0]
            header_count = first_line.count(',') + 1
            
            # Read the CSV with pandas, specifying the number of columns
            df = pd.read_csv(
                io.StringIO(csv_content),
                usecols=range(header_count),
                parse_dates=['Date'],
                dayfirst=True,
                on_bad_lines='warn',
                encoding_errors='replace'  # replace characters that can't be decoded
            )
            
            # print(f"successfully read with encoding: {encoding}")
            return df
            
        except Exception as e:
            print(f"reading/parsing csv file content failed with encoding {encoding}: {str(e)}")
            continue
    
    # If we've tried all encodings and none worked
    raise ValueError("could not read the csv file with any encoding")

countries = pd.read_csv(DATA_PATH+COUNTRY_LIST)['country']

print('fetching data files for', len(countries), 'countries')

for country in countries:
    datafile_urls = pd.read_csv(data_path+'config/'+country+'_data_file_urls.csv')
    print(country, 'has', len(datafile_urls), 'urls')
    for datafile_url in datafile_urls['data_url']:
        print('reading', datafile_url)
        df = read_problematic_csv(datafile_url)
        df['country'] = country
        filename =                                \
            data_path                             \
            + 'raw/'                              \
            + country                             \
            + '_'                                 \
            + datafile_url                        \
                .split('football-data.co.uk/')[1] \
                .replace('/', '_')                \
                .split('.')[0]                    \
            + '.csv'                              \
            .lower()
        df.to_csv(filename)

