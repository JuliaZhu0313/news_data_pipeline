# streamlit_app.py
from pandas.core.arrays.integer import Int64Dtype
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import seaborn as sns
import matplotlib.pyplot as plt
import datetime
from PIL import Image
import os
import numpy as np
import pandas as pd
import plotly.express as px
from langdetect import detect
from wordcloud import WordCloud
import plotly.graph_objects as go
import re
#import nltk
#nltk.download('stopwords')
#from nltk.corpus import stopwords
import string

# Page config
st.set_page_config(page_icon="💰", page_title="InvestorInsight", layout="wide")

client = bigquery.Client()
@st.cache_data

#Function to read data from BigQuery
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return rows


#Load total data
dataset_id = 'raw_data'
dataset_ref = client.dataset(dataset_id)
stock_price_id = 'StockData'
table_ref = dataset_ref.table(stock_price_id)
table = client.get_table(table_ref) 
stock_price_query = run_query(f"SELECT * FROM `{dataset_id}.{stock_price_id}`")
stock_price = pd.DataFrame(stock_price_query)
news_id = 'News'
news_query = run_query(f"SELECT * FROM `{dataset_id}.{news_id}`")
news = pd.DataFrame(news_query)
finance_situation_id = 'FinanceSituation'
finance_situation_query = run_query(f"SELECT * FROM `{dataset_id}.{finance_situation_id}`")
finance_situation = pd.DataFrame(finance_situation_query)
company_id = 'Company'
company_query = run_query(f"SELECT * FROM `{dataset_id}.{company_id}`")
company = pd.DataFrame(company_query)
ticker_id = 'Tickers'
ticker_query = run_query(f"SELECT * FROM `{dataset_id}.{ticker_id}`")
ticker = pd.DataFrame(ticker_query)
industry_id = 'Industry'
industry_query = run_query(f"SELECT * FROM `{dataset_id}.{industry_id}`")
industry = pd.DataFrame(industry_query)
industry = industry.set_index('Industry')
company_list = list(stock_price['Ticker'].unique())
page = st.sidebar.selectbox(
    "Select your interested page",
    ('Company Analysis', 'News Summary', 'Industry Overview'))
st.title("Investor Insight 📈")
#page = 'Company Analysis'
st.markdown('''
            <style>
            .st-c7 {
                flex-shrink: 0;
                position: absolute;
                opacity: 0;
                cursor: pointer;
                height: 0;
                width: 0;
            }
            </style>
            ''', unsafe_allow_html=True)

def news_transform(text):
    text = text.split(' ')
    stop_words = {
    'a', 'about', 'above', 'after', 'again', 'against', 'ain', 'all', 'am', 'an', 'and', 'any', 'are', 'aren',
    "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by',
    'can', 'couldn', "couldn't", 'd', 'did', 'didn', "didn't", 'do', 'does', 'doesn', "doesn't", 'doing', 'don',
    "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadn', "hadn't", 'has', 'hasn',
    "hasn't", 'have', 'haven', "haven't", 'having', 'he', 'her', 'here', 'hers', 'herself', 'him', 'himself',
    'his', 'how', 'i', 'if', 'in', 'into', 'is', 'isn', "isn't", 'it', "it's", 'its', 'itself', 'just', 'll',
    'm', 'ma', 'me', 'mightn', "mightn't", 'more', 'most', 'mustn', "mustn't", 'my', 'myself', 'needn',
    "needn't", 'no', 'nor', 'not', 'now', 'o', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'our', 'ours',
    'ourselves', 'out', 'over', 'own', 're', 's', 'same', 'shan', "shan't", 'she', "she's", 'should', "should've",
    'shouldn', "shouldn't", 'so', 'some', 'such', 't', 'than', 'that', "that'll", 'the', 'their', 'theirs', 'them',
    'themselves', 'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up',
    've', 'very', 'was', 'wasn', "wasn't", 'we', 'were', 'weren', "weren't", 'what', 'when', 'where', 'which',
    'while', 'who', 'whom', 'why', 'will', 'with', 'won', "won't", 'wouldn', "wouldn't", 'y', 'you', "you'd",
    "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves'
    }
    #stop_words = set(stopwords.words('english'))
    final_text = ''
    for t in text:
        if t in stop_words:
            continue
        if t in string.punctuation:
            continue
        final_text += t
        final_text += ' '
    return final_text


if page == 'Company Analysis':
    col1, col2 = st.columns([2,1])
    with col1:
        selected_company = st.selectbox('Choose the company for analytics',  company_list, label_visibility = 'visible')
    with col2:
        selected_figure = st.selectbox('Choose the chart for visualization', ['Company Information','Stock Return','Market Indexes', 'Word Cloud'], index = 0)

    if selected_company == None:
        st.write(f"Please select the company for analysis")
    else:
        st.write(f"Selected Company: {selected_company}")

        company_info = company[company['id'] == selected_company]
        name = company_info['name'].iloc[0]
        employee = company_info['fullTimeEmployees'].iloc[0]
        current_industry = company_info['Industry'].iloc[0]
        country = company_info['Country'].iloc[0]
        related_news_id = []
        for i in range(len(ticker)):
            if ticker['ticker'].iloc[i] == selected_company:
                related_news_id.append(ticker['news_id'].iloc[i])
            related_news = news[news['id'].isin(related_news_id)]
        if len(related_news) > 0:  
            total_news = ''.join(list(related_news['text']))
            total_news = news_transform(total_news)
            average_sentiment = sum(list(related_news['sentiment']))/len(related_news)
        else:
            total_news = ''
            average_sentiment = 0

        if selected_figure == 'Company Information': 
            st.write('</div>', unsafe_allow_html=True)
            st.write(f"Company name: **{name}**")
            st.write(f"Company full time employee numbers: **{employee}**")
            st.write(f"Industry: **{current_industry}**")
            st.write(f'Country: **{country}**')
        if selected_figure == 'Stock Return':                                                        
            c1 = st.container()
            with c1:
                selected_stock = stock_price[stock_price['Ticker'] == selected_company][1:]
                fig = px.line(selected_stock, x='Date', y='log_return', title='Log Return Over Time')
                fig.update_traces(mode='markers+lines', marker=dict(size=8))
                fig.update_layout(hovermode='x',width=1300, height=1000)
                st.plotly_chart(fig)
            st.markdown("""---""")
        if selected_figure == 'Market Indexes':
            c4 = st.container()
            with c4:
                situation = finance_situation[finance_situation['company_id'] == selected_company]
                filtered_situation = {key: situation[key].iloc[0] for key in list(situation.columns) if isinstance(situation[key].iloc[0], float) and key!='company_id'}
                filtered_situation['sentiment'] = average_sentiment
                industry_info = industry.loc[current_industry]
                industry_data = []
                for key in list(filtered_situation.keys()):
                    industry_data.append(industry_info[key])
                x_values = list(filtered_situation.keys())
                company_data = list(filtered_situation.values())
                trace1 = go.Bar(x=x_values, y=company_data, name=f'{name}')
                trace2 = go.Bar(x=x_values, y = industry_data, name=f'{current_industry}')
                layout = go.Layout(title=f'Financial Data Comparing company {name} and the industry {current_industry}', xaxis_title='Index', yaxis_title='Value',  height=800, width=1300)
                fig = go.Figure(data=[trace1, trace2], layout=layout)
                st.plotly_chart(fig)
            st.markdown("""---""")
        if selected_figure == 'Word Cloud':
            c3 = st.container()
            with c3:
                if len(total_news) > 0:
                    wordcloud = WordCloud(width=800, height=400,background_color = 'black').generate(total_news)
                    fig = go.Figure(go.Image(z=np.array(wordcloud)))
                    fig.update_layout(title=f'Word Cloud of news related to {name}', width=1300, height=800)
                    st.plotly_chart(fig)
                else:
                    st.markdown("""No related news about this company""")
if page == 'Industry Overview':
    c3 = st.container()
    with c3:
        indexes = [''] + list(industry.columns)
        industries = [''] + list(industry.index)
        col1, col2 = st.columns([2,1])
        with col1:
            selected_attribute = st.selectbox('Choose the index for comparison',  indexes, label_visibility = 'visible')
        with col2:
            selected_industry = st.selectbox('Choose the industry for comparison',  industries, label_visibility = 'visible')
        if selected_attribute == '':
            if selected_industry == '':
                st.markdown("""Enter a specific industry or index for analysis""")
            else:
                x_values = list(industry.columns)
                y_values = list(industry.loc[selected_industry])
                layout = go.Layout(title=f'Comparing different indexes with {selected_industry}', xaxis_title='Index', yaxis_title='Value',  height=800, width=1300)
                trace = go.Bar(x=x_values, y=y_values)
                fig = go.Figure(data=trace, layout=layout)
                st.plotly_chart(fig)    
        else:
            if selected_industry == '':
                y_values = list(industry[selected_attribute])
                x_values  = list(industry.index)
                layout = go.Layout(title=f'Comparing different industries with {selected_attribute}', xaxis_title='Industry', yaxis_title='Value',  height=800, width=1300)
                trace = go.Bar(x=x_values, y=y_values)
                fig = go.Figure(data=trace, layout=layout)
                st.plotly_chart(fig)                      
            else:
                related_companies = list(company[company['Industry'] == selected_industry]['id'])
                if selected_attribute == 'sentiment':
                    y_values = []
                    for id in related_companies:
                        related_news_id = []
                        for i in range(len(ticker)):
                            if ticker['ticker'].iloc[i] == id:
                                related_news_id.append(ticker['news_id'].iloc[i])
                        related_news = news[news['id'].isin(related_news_id)]
                        if len(related_news) > 0:  
                            average_sentiment = sum(list(related_news['sentiment']))/len(related_news)
                        else:
                            average_sentiment = 0
                        y_values.append(average_sentiment)
                else:
                    y_values = list(finance_situation[finance_situation['company_id'].isin(related_companies)][selected_attribute])
                x_values = related_companies
                layout = go.Layout(title=f'Comparing the {selected_attribute} feature in the {selected_industry} industry', xaxis_title='Index', yaxis_title='Value',  height=800, width=1300)
                trace = go.Bar(x=x_values, y=y_values)
                fig = go.Figure(data=trace, layout=layout)
                st.plotly_chart(fig)        
if page == 'News Summary':
    col1, col2 = st.columns([2,1])
    with col1:
        selected_news_data = st.selectbox('Choose the news analysis aspect', ['Word Cloud', 'News Count', 'Source Country', 'Sentiment Summary'], label_visibility = 'visible')
        if selected_news_data == 'Word Cloud':
            total_news_text = ''.join(list(news['text']))
            total_news_text = news_transform(total_news_text)
            wordcloud = WordCloud(width=800, height=400,background_color = 'black').generate(total_news_text)
            fig = go.Figure(go.Image(z=np.array(wordcloud)))
            fig.update_layout(title=f'Word Cloud of total news', width=1300, height=800)
            st.plotly_chart(fig)
        if selected_news_data == 'News Count':
            date_counts = news['publish_date'].value_counts().to_dict()
            y_values = list(date_counts.values())
            x_values = list(date_counts.keys())
            layout = go.Layout(title=f'News Count of different dates', xaxis_title='Date', yaxis_title='Count',  height=800, width=1300)
            trace = go.Bar(x=x_values, y=y_values)
            fig = go.Figure(data=trace, layout=layout)
            st.plotly_chart(fig)
        if selected_news_data == 'Source Country':
            country_counts = news['source_country'].value_counts().to_dict()
            y_values = list(country_counts.values())
            x_values = list(country_counts.keys())
            layout = go.Figure( data=[go.Pie(labels=x_values, values=y_values)])
            layout.update_layout(title='News Count of different countries')
            st.plotly_chart(layout)
        if selected_news_data == 'Sentiment Summary':
            sentiment_counts = news.groupby('publish_date')['sentiment'].mean().to_dict()
            y_values = list(sentiment_counts.values())
            x_values = list(sentiment_counts.keys())
            fig = go.Figure(data=go.Scatter(x=x_values, y=y_values, mode='lines+markers'))

            fig.update_layout(title='Average news sentiment across time', xaxis_title='Date', yaxis_title='Sentiment',height=800, width=1300)
            st.plotly_chart(fig)