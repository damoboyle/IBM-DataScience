import requests
from bs4 import BeautifulSoup
import pandas
import sqlite3
from datetime import datetime

#Known Variables
url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
csv = './Countries_by_GDP.csv'
table = 'Countries_by_GDP'
db = 'World_Economies.db'
attributes = ['Country', 'GDP_USD_billion']
log = './etl_project_log.txt'

# Code for ETL operations on Country-GDP data
def extract(url, attributes):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing.'''

    df = pandas.DataFrame(columns = attributes)

    html = requests.get(url).text
    data = BeautifulSoup(html, 'html.parser')
    tables = data.find_all('tbody')
    for row in tables[2].find_all('tr'):
        cell = row.find_all('td')
        if len(cell) != 0:
            if '—' not in cell[2] and cell[0].find('a') is not None:
                data_dict = {'Country': cell[0].a.contents[0], 'GDP_USD_billion':cell[2].contents[0]}
                cdf = pandas.DataFrame(data_dict, index=[0])
                df = pandas.concat([df, cdf],  ignore_index=True)

    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    df['GDP_USD_billion'] = ((df['GDP_USD_billion'].str.replace(',','').astype(float))/1000).round(2)

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)

def load_to_db(df, conn, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, conn, if_exists='replace', index=False)

def run_query(query_statement, conn):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    print(pandas.read_sql(query_statement, conn))

def log_progress(log, message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

    now = datetime.now() 
    timestamp = now.strftime('%Y-%h-%d %H:%M:%S')
    with open(log, 'a') as log_file:
        log_file.write(f"{timestamp} : {message}\n")

#Function Calls
log_progress(log, 'Preliminaries complete. Initiating ETL process')
df = extract(url, attributes)
log_progress(log, 'Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress(log, 'Data transformation complete. Initiating loading process')
load_to_csv(df, csv)
log_progress(log, 'Data saved to CSV file')
conn = sqlite3.connect(db)
log_progress(log, 'SQL Connection initiated.')
load_to_db(df, conn, table)
log_progress(log, 'Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table} WHERE GDP_USD_billion >= 100"
run_query(query_statement, conn)
log_progress(log, 'Process Complete.')
conn.close()