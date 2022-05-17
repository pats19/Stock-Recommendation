#!/usr/bin/env python
# coding: utf-8

# In[1]:


from sqlalchemy import dialects
import pandas as pd
import numpy as np
import pymysql
import yfinance as yf


# pip install pandas

# In[2]:


import sqlalchemy


# In[3]:


wiki="https://en.wikipedia.org/wiki/"


# In[4]:


tickers_nifty_next50 = pd.read_html(wiki+"NIFTY_Next_50")[1].Symbol.to_list()
tickers_nifty50 = pd.read_html(wiki+"NIFTY_50")[1].Symbol.to_list()


# In[5]:


tickers_nifty50 = [i+ ".NS" for i in tickers_nifty50]
tickers_nifty_next50 = [i+ ".NS" for i in tickers_nifty_next50]


# In[6]:


tickers_nifty50


# In[7]:


import datetime as dt


# def getdata(tickers):
#     data = []
#     for ticker in tickers:
#         data.append(yf.download(ticker,start = "2022-01-01",end = dt.datetime.today()).reset_index())
#     return data

# nifty50 = getdata(tickers_nifty50)

# nifty_next50 = getdata(tickers_nifty_next50)

# In[8]:


indices =["nifty_50","nifty_next_50"]  


# In[9]:


pymysql.install_as_MySQLdb()


# In[10]:


def schema_creator(index):
    engine = sqlalchemy.create_engine("mysql://root:12345@127.0.0.1:3306/")
    engine.execute(sqlalchemy.schema.CreateSchema(index))


# In[11]:


for index in indices:
    schema_creator(index)


# In[12]:


print("tickers nifty 50")
print(tickers_nifty50)
print("--------------------------")
print("tickers_nifty_next_50")
print(tickers_nifty_next50)


# In[13]:


mapper = {"nifty_50":tickers_nifty50,"nifty_next_50":tickers_nifty_next50}


# In[14]:


for index in indices:
    engine = sqlalchemy.create_engine("mysql://root:12345@127.0.0.1:3306/"+index)
    for symbol in mapper[index]:
        df = yf.download(symbol,start = "2021-05-01",end = dt.datetime.today())
        df = df.reset_index()
        df.to_sql(symbol,engine)

