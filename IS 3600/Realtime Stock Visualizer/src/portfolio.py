from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float
import pandas as pd
import psycopg2 as psql


def insert_portfolio(engine):
    df = pd.read_csv('portfolio.csv', on_bad_lines='skip')

    df.drop(columns=['Account Number', 
                    'Description',
                    'Last Price Change',
                    "Today's Gain/Loss Dollar",
                    "Today's Gain/Loss Percent",
                    "Total Gain/Loss Dollar",
                    "Total Gain/Loss Percent",
                    "Percent Of Account",
                    "Type"], inplace=True)

    index_of_pending = df[df['Symbol'] == 'Pending Activity'].index[0]
    df = df.iloc[:index_of_pending]

    df = df.replace({r'\$': ''}, regex=True)


    df.columns = df.columns.str.lower().str.replace(' ', '_')


    df.to_sql('portfolio', engine, if_exists='replace', index=False)


