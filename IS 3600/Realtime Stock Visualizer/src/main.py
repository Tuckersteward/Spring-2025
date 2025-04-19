import yfinance as yf
import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as psql
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

# Importing functions from other modules
from get_tickers import gather_tickers, existing_tickers, insert_ticker_data
from individual_companies import gather_company_data
from etf__holdings import get_etf_holdings
from build_tables import build_tables
from portfolio import insert_portfolio
from financials import get_financials

DB_CONFIG = {
    "dbname": "postgres",  
    "user": "postgres",    
    "password": "password",  
    "host": "localhost", 
    "port": "5432" 
}

engine = create_engine('postgresql://postgres:password@localhost:5432/postgres')


path = os.path.join('..', 'data')
if not os.path.exists(path):
    os.makedirs(path)

def main():
    build_tables(DB_CONFIG)  
    
    insert_portfolio(engine)

    gather_tickers(DB_CONFIG)  

    insert_ticker_data(DB_CONFIG, portfolio=True)
    
    gather_company_data(DB_CONFIG) 

    get_etf_holdings(DB_CONFIG)
    
    get_financials(DB_CONFIG)

if __name__ == "__main__":
    main()
