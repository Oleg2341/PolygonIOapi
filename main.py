import datetime
import pandas as pd
from sqlalchemy import create_engine
from parser_1 import PolygonAPI

df=PolygonAPI().get_klines(
    symbol='AAPL',
    interval=1,
    unit='minute',
    start=datetime.datetime(year=2024, month=2, day=2),
    end=datetime.datetime(year=2024, month=2, day=7),
)


table_name = 'aapl_klines'

engine = create_engine('postgresql+psycopg2://postgres:rootroot@localhost/postgres')
df.to_sql(table_name, engine, if_exists='replace', index=False)