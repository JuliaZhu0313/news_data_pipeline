from airflow.decorators import dag,task
from datetime import datetime,timedelta
import sys 
import os 
import pandas as pd
import yfinance as yf
import requests_cache
import numpy as np
import json
from json.decoder import JSONDecodeError
import requests
import nltk
from nltk.corpus import stopwords
import string

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='Credit_Risk_Analysis',default_args=default_args,schedule=None,tags=['Project'])
def  Credit_Risk_Analysis_taskflow_api_etl():
    @task(task_id = 'Company_Extract')
    def company_extract():
        companies_df = pd.read_csv("datasets/constituents.csv")
        company_df = pd.DataFrame(columns=['id', 'name', 'fullTimeEmployees', 'Industry', 'Country'])
        #read in companies to use 
        company_tickers = companies_df["Symbol"].tolist()
        company_rawinfo = []
        for ticker in company_tickers:
            try:
                company_dict = {"id":ticker}
                company = yf.Ticker(ticker)
                information = company.info
                company_rawinfo.append(information)
                company_dict.update({'name':information.get('shortName', None), 'fullTimeEmployees':information.get('fullTimeEmployees', None),
                            'Industry':information.get('sector', None), 'Country':information.get('country', None)})
                company_df = pd.concat([company_df, pd.DataFrame([company_dict])], ignore_index=True)
            except Exception as e:
                print(f"Error retrieving data for {ticker}: {e}")
        
        company_rawfile = 'Company/Company_extract.csv'
        company_df.to_csv(company_rawfile, index=False) 
        return company_rawfile
    
    @task(task_id = 'Company_Transform')
    def company_transform(company_rawfile):
        company_df = pd.read_csv(company_rawfile)
        company_df['fullTimeEmployees'] = company_df['fullTimeEmployees'].astype(float)
        company_file = 'Company/Company.csv'        
        company_df.to_csv(company_file, index=False) 
        return company_file
    
    @task(task_id = 'Finance_Extraction')
    def finance_extraction():
        #read in companies to use 
        companies_df = pd.read_csv("datasets/constituents.csv")
        company_tickers = companies_df["Symbol"].tolist()
        finance_situation_df = pd.DataFrame(columns=['company_id', 'AuditRisk', 'Dividend rate', 'Dividend Yield', 
                                             'Payout rate', 'Beta', 'Market Cap', 'profit margins', 'short ratio', 
                                             'quick ratio', 'current ratio', 'debtToEquity'])
        for ticker in company_tickers:
            try:
                finance_dict = {"company_id":ticker}
                company = yf.Ticker(ticker)
                information = company.info
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

        finance_rawfile = 'Finance_Situation/Finance_Situation_raw.csv'
        finance_situation_df.to_csv(finance_rawfile, index = False)
        return finance_rawfile
    
    @task(task_id = 'Finance_Transform')
    def finance_transform(company_file,finance_rawfile):
        company_df = pd.read_csv(company_file)
        finance_situation_df = pd.read_csv(finance_rawfile)
        columns = finance_situation_df.columns.tolist()
        finance_situation_df[columns[1:]]= finance_situation_df[columns[1:]].astype(float)
        # calculate market cap ratio
        merged_df = pd.merge(company_df, finance_situation_df, left_on='id', right_on='company_id')
        industry_market_cap = merged_df.groupby('Industry')['Market Cap'].sum()
        finance_situation_df['debtToEquity'] = finance_situation_df['debtToEquity']/100
        finance_situation_df['Market Cap'] = merged_df.apply(lambda row: (row['Market Cap'] / industry_market_cap[row['Industry']]), axis=1)
        
        finance_file = 'Finance_Situation/Finance_Situation.csv'       
        finance_situation_df.to_csv(finance_file, index = False)
        return finance_file
      
    @task(task_id = 'News_Extraction')
    def news_extractions():
        api_list = [
                    '3bff9734cfbf4e1aa7b60c9fc224452d',
                    'ae48b5cb8ffe44c2b061a25785c74d97',
                    '65e16b9111744ab4840e59166dd659b9',
                    '4a8e398f23024576ae5e29c505e07270',
                    '81f3a64644484e32b542e9f48269b489',
                    '8a7893660af4483ba4da4b18349d6c79',
                    '9a49f2ddb5134355829ac5ae6946d718',
                    'e5b1e9fd2dee4f94aa30f7bfe73f21e4',
                    '0441e2cea6b84a5d976b4c0a037a9e60',
                    '7251ac941bb64e8cbee0eb58f00883a1',
                    'e672029cbbc944b586f436bd12a912d7'
                ]
        companies = pd.read_csv('datasets/constituents.csv')
        company = companies[['Security', 'Symbol']]
        ticker = company['Symbol'].to_list()
        ticker_dict = {}
        for i in ticker:
            ticker_dict[i] = []
        ticker2 = company['Security'].to_list()
        ticker_dict2 = {}
        for i in ticker2:
            ticker_dict2[i] = []
        # Get company_news 
        df = pd.DataFrame()
        empty_list = []
        base_url = "https://api.worldnewsapi.com/search-news"
        counter = 0
        #api_key = "3bff9734cfbf4e1aa7b60c9fc224452d"
        for comp in company['Security']:
            try:
                api_key = api_list[counter]
                name = "ORG: " + comp
                params = {
                "language": "en",  
                "api-key": api_key, 
                # very few weekly data
                "earliest-publish-date":"2024-01-01",
                "latest-publish-date" : "2024-04-01",
                'text': name,
                "number":100
                }
                response = requests.get(base_url, params=params)
                data = response.json()
            except JSONDecodeError:
                print(f"took extremely long to get data for {comp}")
                continue
            try:
                df1 = pd.DataFrame(data['news'])
            except KeyError:
                try:
                    if (data['code']) == 402:
                        if counter == len(api_list) - 1:
                            print(comp)
                            print(api_key)
                            break
                        else:
                            counter = counter + 1
                            api_key = api_list[counter]
                            print(f"update api key to {api_key}")
                            params = {
                            "language": "en",  
                            "api-key": api_key, 
                            # very few weekly data
                            "earliest-publish-date":"2024-01-01",
                            "latest-publish-date" : "2024-04-01",
                            'text': name,
                            "number":100
                            }
                            response = requests.get(base_url, params=params)
                            data = response.json()
                    else:
                        print(data)
                except KeyError:
                    print("No news found")
                    df1 = pd.DataFrame()

            try:
                ticker_dict2[comp] = df1['id'].to_list()
            except KeyError:
                print("No company data")
                empty_list.append(comp)
                ticker_dict2[comp] = []

            df = pd.concat([df, df1], ignore_index=True)
        
        return [df,ticker_dict,ticker_dict2]
    
    @task(task_id='News_Transform')
    def news_transform(data):
        df = data[0]
        ticker_dict = data[1]
        ticker_dict2 = data[2]        
        news_file = "news/news2.csv"
        df.to_csv(news_file, index = False)
        for (key1, value1), (key2, value2) in zip(ticker_dict2.items(), ticker_dict.items()):
            ticker_dict[key2] = value1
        tickers_file = 'company_news/tickers.json'
        with open(tickers_file, "w") as json_file:
            json.dump(ticker_dict, json_file)
        df = pd.read_csv(news_file)
        stop_words = set(stopwords.words('english'))
        texts = df['text']
        final_texts = []
        for text in texts:
            text = text.split(' ')
            final_text = ''
            for t in text:
                if t in stop_words:
                    continue
                if t in string.punctuation:
                    continue
                final_text += t
                final_text += ' '
            final_texts.append(final_text)
        df['text'] = final_texts
        news_transformed = 'news_transformed.csv'
        df.to_csv(news_transformed,index=False)
        return news_transformed
    
    @task(task_id='Stock_Extraction')
    def stock_extract():
        companies = pd.read_csv('datasets/constituents.csv')
        ticker = companies['Symbol'].to_list()
        
        session = requests_cache.CachedSession(cache_name='cache', backend='sqlite')
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
        return stocks
    
    @task(task_id='Stock_Transformation')
    def stock_transform(stocks):        
        companies = pd.read_csv('datasets/constituents.csv')
        ticker = companies['Symbol'].to_list()
        
        for i in ticker:
            data = stocks[i]
            data['Date'] = data['Date'].apply(str)
            data['Date'] = pd.to_datetime(data['Date'], infer_datetime_format=True)
            stocks[i] = data 
            
        df = pd.DataFrame()
        for (key,value) in stocks.items():
            df1 = pd.DataFrame(value)
            df = pd.concat([df, df1], ignore_index=True) 
        
        stock_file = 'stock_price/stock_price.csv'
        df.to_csv(stock_file)
        return stock_file
    
    @task(task_id = 'load_Data')
    def load_all_data(company_file,finance_file,stock_file,news_file):
        project_id="is3107-news"
        tickers_file = "company_news/tickers.json"
        TableId_FilePath_dict={
            "Tickers":tickers_file,
            "Company":company_file,
            "FinanceSituation":finance_file,
            "StockData":stock_file,
            "News":news_file,
        }
        
        # file path after transfering process
        # News and FinanceSituation are transfered
        TableId_FilePath_dict_trans={
            "Tickers":"company_news/tickers.csv",
            "Company":"Company/Company.csv",
            "FinanceSituation":"Finance_situation/Finance_situation_trans.csv",
            "StockData":"stock_price/stock_price.csv",
            "News":"company_news/news2_trans.csv",
        }
        
        TableId_PrimaryKey_dict={
            "Tickers":["ticker","news_id"],
            "Company":["id"],
            "FinanceSituation":["company_id"],
            "StockData":["Ticker","Date"],
            "News":["id"],
        }
        
        relation_columns=["ticker","news_id"]
        # df_tickers.head()
        rows=[]
        with open(TableId_FilePath_dict["Tickers"], 'r') as file:
            data = json.load(file)
            # print(data)
            
            for stock,news_list in data.items():
                for news_id in news_list:
                    rows.append([stock,news_id])
        df_relation=pd.DataFrame(rows,columns=relation_columns)
        df_relation.to_csv(TableId_FilePath_dict_trans["Tickers"],index=False)
        
        #remove the index column in stock_price.csv
        df_stock_price=pd.read_csv(TableId_FilePath_dict["StockData"],usecols=["Ticker","Date","log_return"])
        df_stock_price.to_csv(TableId_FilePath_dict_trans["StockData"],index=False)
        
        # news csv
        # filter chinese character
        df_news=pd.read_csv(TableId_FilePath_dict["News"])

        #time stamp to data time
        df_news["publish_date"]=pd.to_datetime(df_news["publish_date"])
        df_news["publish_date"]=df_news["publish_date"].dt.date
        df_news.to_csv(TableId_FilePath_dict_trans["News"],index=False)

        def contains_chinese(text):
            for char in text:
                if char>='\u4e00' and char<='\u9fff':
                    return True
            return False

        df_filter_title=df_news[~df_news["title"].apply(contains_chinese)]
        df_filter_text=df_news[~df_news["text"].apply(contains_chinese)]
        df_filter_text=df_filter_text.drop_duplicates(subset='id',keep="first")
        df_filter_text.to_csv(TableId_FilePath_dict_trans["News"],index=False)
        
        #finance_situation.csv
        df_fin=pd.read_csv(TableId_FilePath_dict["FinanceSituation"])
        df_fin["debtToEquity"]/=100
        df_fin["debtToEquity"].fillna(0,inplace=True)
        df_fin.to_csv(TableId_FilePath_dict_trans["FinanceSituation"],index=False)

        
        from google.cloud import bigquery

        client = bigquery.Client()

        dataset_id = 'raw_data'
        dataset_ref = client.dataset(dataset_id)
        
        def merge_data(tmp_table_id,target_table_id):

            primarykey_list=TableId_PrimaryKey_dict[target_table_id.split('.')[-1]]
            
            if len(primarykey_list)==1:
                primarykey_statement=f"target.{primarykey_list[0]} = source.{primarykey_list[0]}"
            elif len(primarykey_list)==2:
                primarykey_statement=f"target.{primarykey_list[0]} = source.{primarykey_list[0]} "+\
                    f" AND target.{primarykey_list[1]} = source.{primarykey_list[1]}"
                
            
            merge_statement=f"""
            MERGE `{target_table_id}` AS target
            USING `{tmp_table_id}` AS source
            ON ({primarykey_statement})
            WHEN NOT MATCHED THEN
                INSERT ROW
            """
            
            
            query_job = client.query(merge_statement)

            # 等待任务完成
            query_job.result()

        def load_data(table_id,file_path):
            # this is autodectect schema mode
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
            )
            
            #upload the tmp table
            tmp_table_id=table_id+"_tmp"
            with open(file_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, tmp_table_id, job_config=job_config)
            job.result()  # Waits for the job to complete.
            
            merge_data(tmp_table_id,table_id)
            # drop the tmp table
            client.delete_table(tmp_table_id)
            
            table = client.get_table(table_id)  # Make an API request.
            print(
                "Loaded {} rows and {} columns to {}".format(
                    table.num_rows, len(table.schema), table_id
                )
            )
        
        for table_id,file_path in TableId_FilePath_dict_trans.items():
            print(f"Load data: {table_id}:")
            load_data(f"{project_id}.{dataset_id}.{table_id}",file_path) 
        
        marker = True 
        return marker
    
    @task(task_id='Aggregate_Table')
    def aggregate_table(marker):
        from google.cloud import bigquery
        client = bigquery.Client()
        dataset_id = 'raw_data'
        dataset_ref = client.dataset(dataset_id)
        
        sql_query = '''SELECT
            c.Industry,
            AVG(n.sentiment) AS sentiment,
            AVG(f.AuditRisk) as AuditRisk,
            AVG(f.Dividend_rate) as Dividend_rate,
            AVG(f.Dividend_Yield) as Dividend_Yield,
            AVG(f.Payout_rate) as Payout_rate,
            AVG(f.Beta) as Beta,
            AVG(f.Market_Cap) as Market_Cap,
            AVG(f.profit_margins) as profit_margins,
            AVG(f.short_ratio) AS short_ratio,
            AVG(f.quick_ratio) AS quick_ratio,
            AVG(f.current_ratio) AS current_ratio,
            AVG(f.debtToEquity) AS debtToEquity
        FROM
            `raw_data.FinanceSituation` AS f
        JOIN
            `raw_data.Company` AS c
        ON
            c.id = f.company_id
        JOIN `raw_data.Tickers` AS t
        ON t.ticker = c.id
        JOIN (SELECT * FROM `raw_data.News` WHERE publish_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)) AS n
        ON n.id = t.news_id
        GROUP BY
            c.Industry;'''
            
        destination_table_id = "is3107-news.raw_data.Industry"
        #check existence of table
        try:
            table = client.get_table(destination_table_id)
            table_exists = True
        except:
            table_exists = False
        
        #config to overwrite data in it
        job_config = bigquery.QueryJobConfig(destination=destination_table_id, write_disposition="WRITE_TRUNCATE")
        
        if table_exists:
            query_job = client.query(sql_query, job_config=job_config)
            query_job.result()  # Wait for the query to finish execution
        else: 
            table = bigquery.Table(destination_table_id)
            table = client.create_table(table)
            query_job = client.query(sql_query, job_config=job_config)
            query_job.result()  # Wait for the query to finish execution
    
    company_rawfile = company_extract()
    company_file = company_transform(company_rawfile)
    
    finance_rawfile = finance_extraction()
    finance_file = finance_transform(company_file,finance_rawfile)
    
    data = news_extractions()
    news_file= news_transform(data)
    
    raw_stocks = stock_extract()
    stocks_file = stock_transform(raw_stocks)
    
    marker = load_all_data(company_file,finance_file,stocks_file,news_file)
    
    aggregate_table(marker)
    

Credit_Risk_Analysis_etl_dag = Credit_Risk_Analysis_taskflow_api_etl()