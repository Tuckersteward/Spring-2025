import pandas as pd
from yahooquery import Ticker
import psycopg2 as psql

def insert_summary_details(symbol, db_config):
    conn = psql.connect(**db_config)
    cursor = conn.cursor()

    etf = Ticker(symbol)
    summary = etf.summary_detail.get(symbol, {})

    if isinstance(summary, dict):
        cursor.execute(
            """
            INSERT INTO etf_summary_details (
                etf_symbol, regular_market_price, currency, market_cap, 
                yield, expense_ratio, fifty_two_week_low, fifty_two_week_high, 
                fifty_day_average, two_hundred_day_average, 
                trailing_annual_dividend_rate, trailing_annual_dividend_yield, 
                nav_price, previous_close, open_price, day_low, day_high, volume
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (etf_symbol) DO NOTHING
            """,
            (
                symbol,
                summary.get('regularMarketPreviousClose'),
                summary.get('currency'),
                summary.get('totalAssets'),
                summary.get('yield'),
                summary.get('annualReportExpenseRatio'),
                summary.get('fiftyTwoWeekLow'),
                summary.get('fiftyTwoWeekHigh'),
                summary.get('fiftyDayAverage'),
                summary.get('twoHundredDayAverage'),
                summary.get('trailingAnnualDividendRate'),
                summary.get('trailingAnnualDividendYield'),
                summary.get('navPrice'),
                summary.get('previousClose'),
                summary.get('open'),
                summary.get('dayLow'),
                summary.get('dayHigh'),
                summary.get('volume'),
            )
        )
        conn.commit()
        print(f"Summary details for {symbol} inserted successfully.")
    else:
        print(f"Unexpected data format for {symbol}: {summary}")

    conn.close()

def get_stock_info(ticker, db_config):
    conn = psql.connect(**db_config)
    cursor = conn.cursor()

    link = f"https://www.schwab.wallst.com/schwab/Prospect/research/etfs/schwabETF/index.asp?type=holdings&symbol={ticker}"
    
    df = pd.read_html(link, header=0)[1]
    df.columns = ['symbol', 'Description', 'PortfolioWeight', 'SharesHeld', 'MarketValue']
    df.dropna(subset=['symbol'], inplace=True)
    df['PortfolioWeight'] = df['PortfolioWeight'].replace('%', '', regex=True)
    df['PortfolioWeight'] = pd.to_numeric(df['PortfolioWeight'], errors='coerce')

    for index, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO etf_holdings (etf_symbol, symbol, description, portfolio_weight, shares_held, market_value)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (etf_symbol, symbol) DO NOTHING  -- Prevent duplicate entries
            """,
            (ticker, row['symbol'], row['Description'], row['PortfolioWeight'], row['SharesHeld'], row['MarketValue'])
        )

    conn.commit()

def get_etf_holdings(db_config, tickers=None):
    conn = psql.connect(**db_config)  

    query = "select ticker from tickers where type = 'Mutual Fund'"
    tickers_df = pd.read_sql(query, conn)
    tickers = tickers_df['ticker'].tolist()
    for ticker in tickers:
        insert_summary_details(ticker, db_config) 
        get_stock_info(ticker, db_config) 
