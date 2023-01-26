# Libraries to import
import pandas as pd
import yfinance as yf
import datetime
import sql_functions as sql

# Global variables
OUT_DATE_FORMAT = '%Y%m%d'


def get_price_history_yahoo(ticker, start_date, end_date, interval, store_sql = None):
    historical_prices = yf.download(ticker, start=start_date, end=end_date, interval=interval)
    historical_prices.reset_index(inplace=True)

    if not historical_prices.empty and store_sql:
        # Convert column names to SQL suitable convention
        historical_prices = historical_prices.add_prefix('mkt_')
        conn.create_table("_".join([ticker.replace("-","_"), start_date.strftime(OUT_DATE_FORMAT), end_date.strftime(OUT_DATE_FORMAT)]), historical_prices)
    
    return historical_prices

conn = sql.SqlDb("tcp:AUCLD04018656,1433","team_five_aiml_group")
get_price_history_yahoo("BTC-AUD", datetime.datetime(2020,2,1), datetime.datetime(2023,2,1), "1d", conn)
