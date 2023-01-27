# Libraries to import
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import yfinance as yf
import datetime as dt
import sql_functions as sql
import re


# Global variables
OUT_DATE_FORMAT = '%Y%m%d'
DB_SERVER = "tcp:AUCLD04018656,1433"
DB_NAME = "team_five_aiml_group"
CHARS_TO_REMOVE = ['new Date(',')','[',']','"']


def get_yahoo_price_history(ticker, start_date, end_date, interval, sql_conn = None):
    historical_prices = yf.download(ticker, start=start_date, end=end_date, interval=interval)
    historical_prices.reset_index(inplace=True)

    if not historical_prices.empty and sql_conn:
        # Convert column names to SQL suitable convention
        historical_prices = historical_prices.add_prefix('mkt_')
        sql_conn.create_table("_".join([ticker.replace("-","_"), start_date.strftime(OUT_DATE_FORMAT), end_date.strftime(OUT_DATE_FORMAT)]), historical_prices)

    return historical_prices

def get_supp_date(url, indicator):

    # Retrieve raw data from url and preprocess
    page_content = requests.get(url)
    soup_content = str(page_content.content)
    pre_processed_output = (soup_content.split('[[')[1]).split('{labels')[0][0:-2]

    # Remove special characters from content
    for c in CHARS_TO_REMOVE:
        pre_processed_output = pre_processed_output.replace(c, "")

    # Transform into date, data series
    output_as_list = pre_processed_output.split(',')
    date_list, data_list = output_as_list[::2],output_as_list[1::2]

    # If series match, create dataframe
    if len(date_list)==len(data_list):
        df = pd.DataFrame()
        df['Date'] = pd.to_datetime(date_list)
        df[indicator] = data_list
        return df
    else: return None


if __name__ == "__main__":
    """conn = sql.SqlDb(DB_SERVER, DB_NAME)
    get_yahoo_price_history("BTC-AUD", dt.datetime(2019,2,1), dt.datetime(2023,2,1), "1d", conn)
    conn.__del__ """

    get_supp_date('https://bitinfocharts.com/comparison/size-btc.html', 'avg block size')