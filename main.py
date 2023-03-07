import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker

engine = create_engine('sqlite:///parsed_data.db')
Base = declarative_base()

df = pd.read_excel('1.xlsx', sheet_name='Лист1')
df.columns = ['id',
              'company',
              'fact_Qliq_data1',
              'fact_Qliq_data2',
              'fact_Qoil_data1',
              'fact_Qoil_data2',
              'forecast_Qliq_data1',
              'forecast_Qliq_data2',
              'forecast_Qoil_data1',
              'forecast_Qoil_data2']

df_drop = df.drop([0, 1])

df_drop['date'] = pd.to_datetime('2023-03-01') + pd.to_timedelta(df_drop['id'] - 1, unit='d')

df_drop['total_fact_Qliq'] = df_drop['fact_Qliq_data1'] + df_drop['fact_Qliq_data2']
df_drop['total_fact_Qoil'] = df_drop['fact_Qoil_data1'] + df_drop['fact_Qoil_data2']
df_drop['total_forecast_Qliq'] = df_drop['forecast_Qliq_data1'] + df_drop['forecast_Qliq_data2']
df_drop['total_forecast_Qoil'] = df_drop['forecast_Qoil_data1'] + df_drop['forecast_Qoil_data2']

group_total_fact_Qliq = df_drop.groupby([df_drop['date'].dt.month])['total_fact_Qliq'].mean()
group_total_fact_Qoil = df_drop.groupby([df_drop['date'].dt.month])['total_fact_Qoil'].mean()
group_total_forecast_Qliq = df_drop.groupby([df_drop['date'].dt.month])['total_forecast_Qliq'].mean()
group_total_forecast_Qoil = df_drop.groupby([df_drop['date'].dt.month])['total_forecast_Qoil'].mean()


class Data(Base):
    __tablename__ = 'parsed_data'
    id = Column(Integer, primary_key=True)
    company = Column(String(512))
    fact_Qliq_data1 = Column(Integer())
    fact_Qliq_data2 = Column(Integer())
    fact_Qoil_data1 = Column(Integer())
    fact_Qoil_data2 = Column(Integer())
    forecast_Qliq_data1 = Column(Integer())
    forecast_Qliq_data2 = Column(Integer())
    forecast_Qoil_data1 = Column(Integer())
    forecast_Qoil_data2 = Column(Integer())
    total_fact_Qliq = Column(Integer())
    total_fact_Qoil = Column(Integer())
    total_forecast_Qliq = Column(Integer())
    total_forecast_Qoil = Column(Integer())


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

df_drop.to_sql('parsed_data', engine, if_exists='replace')