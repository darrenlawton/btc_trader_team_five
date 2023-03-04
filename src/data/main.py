# Libraries to import
import market_data as md
import sql_functions as sql
from datetime import date
from datetime import datetime
import pandas as pd


# Global variables
BTC_TICKER  = "BTC-AUD"
PRICE_INT   = "1d"
START_DATE  = datetime(2013,1,1)
END_DATE    = datetime.combine(date.today(), datetime.min.time())
DB_SERVER   = "tcp:AUCLD04018656,1433"
DB_NAME     = "team_five_aiml_group"
SUP_BTC_URL = 'https://bitinfocharts.com/comparison/'
LIST_SUPP_IND = [
    'bitcoin-transaction'
    ,'size-btc'
    ,'sentbyaddress-btc'
    ,'bitcoin-difficulty'
    ,'bitcoin-hashrate'
    ,'bitcoin-mining_profitability'
    ,'sentinusd-btc'
    ,'bitcoin-transactionfees'
    ,'bitcoin-median_transaction_fee'
    ,'bitcoin-confirmationtime'
    ,'transactionvalue-btc'
    ,'mediantransactionvalue-btc'
    ,'tweets-btc'
    ,'google_trends-btc'
    ,'activeaddresses-btc'
    ,'top100cap-btc'
    ,'fee_to_reward-btc'
]


if __name__ == "__main__":
    '''
    To complete
    '''

    # Create SQL Server connection to database
    conn = sql.SqlDb(DB_SERVER, DB_NAME)

    # create table of historical price data for date range 
    md.get_yahoo_price_history(BTC_TICKER, START_DATE, END_DATE, PRICE_INT, conn)

    # create dataframe with supplementary btc data points
    supp_btc_df = pd.DataFrame()
    for i in LIST_SUPP_IND:
        df = md.get_supp_data(SUP_BTC_URL + i + ".html", i, START_DATE, END_DATE)

        # Join into a master dataframe
        if supp_btc_df.empty: supp_btc_df = df
        else: supp_btc_df = pd.merge(supp_btc_df, df, on=[md.SUPP_DATE_COL], how='left')
    
    # Create SQL table with supplementary data
    conn.create_table("_".join(["BTC_SUPP_DATA", START_DATE.strftime(md.OUT_DATE_FORMAT), END_DATE.strftime(md.OUT_DATE_FORMAT)]), supp_btc_df.convert_dtypes())

    conn.__del__
