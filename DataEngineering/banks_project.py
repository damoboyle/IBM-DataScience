import requests
from bs4 import BeautifulSoup
import pandas
import sqlite3
from datetime import datetime

#Known variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv = './Largest_banks_data.csv'
ex_csv = './exchange_rate.csv'
db = 'Banks.db'
table = 'Largest_banks'
log = './code_log.txt'

#Queries
query1 = f'SELECT * FROM {table}'
query2 = f'SELECT AVG(MC_GBP_Billion) FROM {table}'
query3 = f'SELECT Name from {table} LIMIT 5'

#Functions
def log_progress(log, message):
    now = datetime.now()
    stamp = now.strftime('%Y-%h-%d %H:%M:%S')

    with open(log, 'a') as log_file:
        log_file.write(f"{stamp} : {message}\n")

def extract(url):
    bank = []; mc = []

    html = requests.get(url).text
    data = BeautifulSoup(html, 'html.parser')
    tables = data.find_all('tbody')
    for row in tables[0].find_all('tr'):
        cell = row.find_all('td')
        if len(cell) != 0:
            name = cell[1].find_all('a')
            bank.append(name[1].contents[0])
            mc.append(cell[2].contents[0].strip())

    data_dict = {'Name':bank ,'MC_USD_Billion':mc}
    df = pandas.DataFrame(data_dict)

    return df

def transform(df, csv_path):
    ex_df = pandas.read_csv(csv_path)
    print(ex_df.iloc[0][0])
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'].astype(float) * ex_df.loc[0]['Rate']).round(2)
    df['MC_GBP_Billion'] = (df['MC_USD_Billion'].astype(float) * ex_df.loc[1]['Rate']).round(2)
    df['MC_IND_Billion'] = (df['MC_USD_Billion'].astype(float) * ex_df.loc[2]['Rate']).round(2)

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, table, conn):
    df.to_sql(table, conn, if_exists='replace', index=False)

def run_query(query, conn):
    print(query)
    print(pandas.read_sql(query, conn))

log_progress(log, 'Preliminaries complete. Initiating ETL process')

df = extract(url)
log_progress(log, 'Data extraction complete. Initiating Transformation process')

df = transform(df, ex_csv)
log_progress(log, 'Data transformation complete. Initiating Loading process')

load_to_csv(df, csv)
log_progress(log, 'Data saved to CSV file')

conn = sqlite3.connect(db)
log_progress(log, 'SQL Connection initiated')

load_to_db(df, table, conn)
log_progress(log, 'Data loaded to Database as a table, Executing queries')

run_query(query1, conn)
log_progress(log, 'Process Complete')

conn.close()
log_progress(log, 'Server Connection closed')
