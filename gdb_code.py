import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime  

URL = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
TABLE_ATTRIBUTE = ['Country', 'GDP_USD_millions']
DB_NAME = 'World_Economies.db'
TABLE_NAME = 'Countries_by_GDP'
CSV_PATH = 'Countries_by_GDP.csv'

CONNECTION = sqlite3.connect("gdb.db")

def extract():
    response = requests.get(URL)
    html_web = response.text
    html_object = BeautifulSoup(html_web,'html.parser')
    df = pd.DataFrame(columns=TABLE_ATTRIBUTE)

    tables = html_object.find_all('tbody')
    gdb_table = tables[2].find_all('tr')[2:]
    
    for row in gdb_table:
        columns = row.find_all('td')
        country = columns[0].get_text(strip=True)
        imf_estimate = columns[2].get_text(strip=True)
        if imf_estimate == '—' or country == "World":
            continue
        data_dic = {"Country": country, 'GDP_USD_millions': imf_estimate}
        df1 = pd.DataFrame(data=data_dic, index=[0])
        df = pd.concat([df1, df], ignore_index=True)
        print(country, imf_estimate)
    return df
       
       
def transform(df):
    df['GDP_USD_billions'] = round(df['GDP_USD_millions'].str.replace(",", "").astype(float)/1000, 2)
    df.drop(['GDP_USD_millions'], inplace=True, axis=1)
    return df


def transform_v2(df):
    gdp_list = df["GDP_USD_millions"].to_list()
    gdp_list = [float(x.replace(",", "")) for x in gdp_list]
    gdp_list = [np.round(x/1000,2) for x in gdp_list]
    df["GDP_USD_millions"] = gdp_list
    df = df.rename(columns= {"GDP_USD_millions":"GDP_USD_billions"})
    return df



def load(df, csv_path, connection, table_name):
   load_to_csv(df, csv_path)
   load_to_db(df, connection, table_name)

def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=True)


def load_to_db(df, connection, table_name):
    df.to_sql(table_name, connection, if_exists="replace", index=False)

def run_query(query, connection):
    result_df = pd.read_sql_query(query, connection)
    return result_df

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second    
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("gdp_logs.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')


log_progress("ETL process started")
log_progress("EXTRACTION process started")
raw_data = extract()
log_progress("EXTRACTION process finished")
log_progress("Transformed process started")
transformed_data = transform_v2(raw_data)
log_progress("Transformed process finished")
#print(transformed_data)
log_progress("Load process started")
load(transformed_data, CSV_PATH, CONNECTION, TABLE_NAME)
log_progress("Load process finished")

query1 = f"Select * From {TABLE_NAME}"
query2 = f"Select * From {TABLE_NAME} Where GDP_USD_billions > 100"

result1 = run_query(query1, CONNECTION)
log_progress(f"Run query {query1}")
log_progress(f"Result1 {result1}")
result2 = run_query(query2, CONNECTION)
log_progress(f"Run query {query2}")
log_progress(f"Result2 {result2}")

CONNECTION.close()
