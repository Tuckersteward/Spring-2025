import pandas as pd
import psycopg2 as psql 
from datetime import datetime
from yahooquery import Ticker
import os

def gather_tickers(db_config):
    path = os.path.join('..', 'data') 
    file_name = f"gather_tickers_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    file = os.path.join(path, f"{file_name}.csv")
    
    link = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#S&P_500_component_stocks"

    df = pd.read_html(link, header=0)[0]
    df.drop(columns=["Date added", "CIK", "Founded"], inplace=True)
    df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)

    df.rename(columns={"Symbol": "Ticker",
                       "Security": "companyname",
                       "GICS Sector": "Sector",
                       "GICS Sub-Industry": "subsector", 
                       "Headquarters Location": "HeadquartersLocation"
                       }, inplace=True)
    
    df['Type'] = 'Regular'

    df.to_csv(file, index=False)

    conn = psql.connect(**db_config)
    cur = conn.cursor()

    try:
        print("gather tickers before csv", type(file), file)

        with open(file, 'r') as f:
            print(f"Copying from file {file}, type: {type(file)}")

            for index, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO tickers (ticker, companyname, sector, subsector, HeadquartersLocation, type)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker) DO NOTHING
                    """, 
                    (row['Ticker'], row['companyname'], row['Sector'], row['subsector'], row['HeadquartersLocation'], row['Type'])
                )
        
        conn.commit()

        os.remove(file)

        print("Data inserted successfully into PostgreSQL!")
    except Exception as e:
        print(f"Error during inserting data: {e}")
    finally:
        conn.close()

def existing_tickers(db_config):
    conn = psql.connect(**db_config)  
    query = "SELECT ticker FROM tickers;"
    # removed  WHERE type = 'Regular'
    df = pd.read_sql(query, conn)
    conn.close()
    return df['ticker'].tolist()

def clean_value(value):
    """Return None if the value is missing or empty."""
    return value if value and value != "N/A" else None

def insert_ticker_data(db_config, tickers=None, portfolio=False):
    try:
        with psql.connect(**db_config) as conn:
            cursor = conn.cursor()

            if portfolio:
                query = """
                    SELECT symbol 
                    FROM portfolio p
                    WHERE symbol NOT LIKE '%**%' 
                    AND symbol NOT IN (SELECT ticker FROM tickers);
                """
                df = pd.read_sql_query(query, conn)
                tickers = df['symbol'].tolist()

            modules = 'assetProfile defaultKeyStatistics'
            tickers_data = Ticker(tickers).get_modules(modules)

            for symbol, data in tickers_data.items():
                try:
                    asset_profile = data.get("assetProfile", {})
                    key_stats = data.get("defaultKeyStatistics", {})

                    sector = clean_value(asset_profile.get("sector"))
                    subsector = clean_value(asset_profile.get("industry"))
                    headquarters = clean_value(asset_profile.get("address1"))
                    is_mutual_fund = (
                        "Mutual Fund" if key_stats.get("legalType") == "Exchange Traded Fund" else "Stock"
                    )

                    cursor.execute(
                        """
                        INSERT INTO tickers (ticker, sector, subsector, headquarterslocation, type)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (ticker) DO NOTHING
                        """,
                        (symbol, sector, subsector, headquarters, is_mutual_fund)
                    )
                    print(f"Inserted {symbol} successfully.")

                except Exception as inner_e:
                    print(f"Error inserting {symbol}: {inner_e}")

            conn.commit()

    except Exception as e:
        print(f"Error processing tickers: {e}")