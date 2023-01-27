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
CHARS_TO_REMOVE = ['new Date(',')','[',']','"']
SUPP_DATE_COL = "supp_date"

def get_yahoo_price_history(ticker, start_date, end_date, interval, sql_conn = None):
    '''
    To complete
    '''
    historical_prices = yf.download(ticker, start=start_date, end=end_date, interval=interval)
    historical_prices.reset_index(inplace=True)

    if not historical_prices.empty and sql_conn:
        # Convert column names to SQL suitable convention
        historical_prices = historical_prices.add_prefix('mkt_')
        sql_conn.create_table("_".join([ticker.replace("-","_"), start_date.strftime(OUT_DATE_FORMAT), end_date.strftime(OUT_DATE_FORMAT)]), historical_prices)

    return historical_prices

def get_supp_data(url, indicator, start_date = pd.Timestamp.min, end_date = pd.Timestamp.max):
    '''
    To complete
    '''
    # Retrieve raw data from url and preprocess
    try:
        page_content = requests.get(url)
        soup_content = str(page_content.content)
        pre_processed_output = (soup_content.split('[[')[1]).split('{labels')[0][0:-2]
    except Exception as error:
        print("Could not request content from {}".format(url))

    # Remove special characters from content
    for c in CHARS_TO_REMOVE:
        pre_processed_output = pre_processed_output.replace(c, "")

    # Transform into date, data series
    output_as_list = pre_processed_output.split(',')
    date_list, data_list = output_as_list[::2],output_as_list[1::2]

    # If series match, create dataframe
    if len(date_list)==len(data_list):
        df = pd.DataFrame()
        df[SUPP_DATE_COL] = pd.to_datetime(date_list)
        df[indicator] = data_list
        return df[df[SUPP_DATE_COL].between(start_date, end_date)] # filter for input dates
    else: return None

