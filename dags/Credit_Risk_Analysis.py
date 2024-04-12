from airflow.decorators import dag,task
from datetime import datetime,timedelta
import sys 
import os 
import pandas as pd
import yfinance as yf
import requests_cache
import pandas as pd
import numpy as np

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='Credit_Risk_Analysis',default_args=default_args,schedule=None,tags=['Project'])
def  Credit_Risk_Analysis_taskflow_api_etl():
    @task(task_id = 'Company_Finance_Retreive')
    def company_finance():
        companies_df = pd.read_csv("../datasets/constituents.csv")
        #read in companies to use 
        company_tickers = companies_df["Symbol"].tolist()
        # Initialise two dataframes
        company_df = pd.DataFrame(columns=['id', 'name', 'fullTimeEmployees', 'Industry', 'Country'])
        finance_situation_df = pd.DataFrame(columns=['company_id', 'AuditRisk', 'Dividend rate', 'Dividend Yield', 
                                                    'Payout rate', 'Beta', 'Market Cap', 'profit margins', 'short ratio', 
                                                    'quick ratio', 'current ratio', 'debtToEquity'])
        
        # Retrieving information and append to data frame for each ticker
        for ticker in company_tickers:
            try:
                company_dict = {"id":ticker}
                finance_dict = {"company_id":ticker}
                company = yf.Ticker(ticker)
                information = company.info
                company_dict.update({'name':information.get('shortName', None), 'fullTimeEmployees':information.get('fullTimeEmployees', None),
                                    'Industry':information.get('sector', None), 'Country':information.get('country', None)})
                company_df = pd.concat([company_df, pd.DataFrame([company_dict])], ignore_index=True)
                finance_dict.update({'AuditRisk': information.get('auditRisk', None), 
                                    'Dividend rate': information.get('dividendRate', None), 
                                    'Dividend Yield': information.get('dividendYield', None),
                                    'Payout rate': information.get('payoutRatio', None), 
                                    'Beta': information.get('beta', None), 
                                    'Market Cap': information.get('marketCap', None),
                                    'profit margins': information.get('profitMargins', None), 
                                    'short ratio': information.get('shortRatio', None),
                                    'quick ratio': information.get('quickRatio', None), 
                                    'current ratio': information.get('currentRatio', None),
                                    'debtToEquity': information.get('debtToEquity', None)})
                finance_situation_df = pd.concat([finance_situation_df, pd.DataFrame([finance_dict])], ignore_index=True)
            except Exception as e:
                print(f"Error retrieving data for {ticker}: {e}")
        
        # Convert data types
        company_df['fullTimeEmployees'] = company_df['fullTimeEmployees'].astype(float) 
        columns = finance_situation_df.columns.tolist()
        finance_situation_df[columns[1:]]= finance_situation_df[columns[1:]].astype(float)

        # calculate market cap ratio
        merged_df = pd.merge(company_df, finance_situation_df, left_on='id', right_on='company_id')
        industry_market_cap = merged_df.groupby('Industry')['Market Cap'].sum()
        finance_situation_df['Market Cap'] = merged_df.apply(lambda row: (row['Market Cap'] / industry_market_cap[row['Industry']]) * 100, axis=1)
        
        # store 2 csv files
        company_df.to_csv('../company/Company.csv', index=False) 
        finance_situation_df.to_csv('../Finance_Situation/Finance_Situation.csv', index = False)
    
    @task(task_id='Stock_extraction')
    def stock_data():
        companies = pd.read_csv('../datasets/constituents.csv')
        ticker = companies['Symbol'].to_list()
        
        session = requests_cache.CachedSession(cache_name='../stock_price/cache', backend='sqlite')
        session.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
                        'Accept': 'application/json;charset=utf-8'}

        # Remember to change start_date and end_date in DAG
        def get_data_for_multiple_stocks(tickers, start_date='2024-01-01',end_date= '2024-04-01', session= session):
            stocks = dict()
            for ticker in tickers:
                data = yf.download(ticker, start=start_date, end=end_date, session=session)
                data.insert(0, 'Ticker', ticker)
                data = data.reset_index()
                data['Prev Close'] = data['Adj Close'].shift(1)
                data['log_return'] = np.log(data['Adj Close'] / data['Prev Close'])
                df1 = data[['Ticker', 'Date', 'log_return']]
                stocks[ticker] = df1
            return stocks

        stocks = get_data_for_multiple_stocks(ticker)
        
        # data type transform
        for i in ticker:
            data = stocks[i]
            data['Date'] = data['Date'].apply(str)
            data['Date'] = pd.to_datetime(data['Date'], infer_datetime_format=True)
            stocks[i] = data
            
        df = pd.DataFrame()
        for (key,value) in stocks.items():
            df1 = pd.DataFrame(value)
            df = pd.concat([df, df1], ignore_index=True)
        df.to_csv('../stock_price/stock_price.csv')
    company_finance()  
    stock_data()   

Credit_Risk_Analysis_etl_dag = Credit_Risk_Analysis_taskflow_api_etl()