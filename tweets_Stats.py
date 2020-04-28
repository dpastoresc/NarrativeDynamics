# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np


import mysql.connector
from sqlalchemy import create_engine

import nltk
nltk.download('punkt')
nltk.download('stopwords')

import re
from nltk.corpus import stopwords
import string
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
from nltk.stem import SnowballStemmer

import pickle
import itertools

import networkx as nx
import time
from datetime import datetime, timedelta, date
from timeit import default_timer as timer

from sys import argv

tag=''

#Se pasa como argumento el nombre de la tabla de la base de datos a procesar
db_name_table = 'PostsCorMad'#str(argv[1])
datapath='/home/davidpastor/Narrativas/CorMad/'
db_name_table = 'PostsMadCar'#str(argv[1])
datapath='/home/davidpastor/Narrativas/MadCar/'
m_database='TwitterDisaster'
m_database='twitterdb'

keywords_list = ['descarbonización','descarbonizacion','clima','climático','climatico','combustible', 'CO2', 'climática', 'climatica', 'transición energética', 'renovable', 'energía', 'energia', 'energético', 'energética', 'energetico', 'energetica']
keywords_list = ['coronavirus', 'Coronavirus', '#CoronavirusES', 'coronavirusESP', '#coronavirus', '#Coronavirus','covid19', '#covid19','Covid19', '#Covid19', 'covid-19', '#covid-19', 'COVID-19', '#COVID-19']
#keywords_list = ['ODS', 'sostenibilidad', 'desarrollo', 'sostenible', 'cooperación']
geo = [-3.7475842804,40.3721683069,-3.6409114868,40.4886258195] #madrid

m_user='david'
m_pass='password'
address='192.168.0.154'
address='127.0.0.1:3306'
encoding = 'utf-8'
            
print(type(str (db_name_table)))
start=timer()

engine = create_engine('mysql+mysqlconnector://'+m_user+":"+m_pass+'@'+address+'/'+m_database,pool_recycle=3600)

#Reading database table to a dataframe

query = 'SELECT COUNT(*) FROM '+ db_name_table
data = pd.read_sql(query, engine)
nrows=data["COUNT(*)"][0]

init=0
init=nrows-10

dflist=[]

for i in range(init, nrows, 1000):
    data  = pd.read_sql("SELECT * FROM "+db_name_table+ " LIMIT "+str(i)+",1000", engine)

    df = data.loc[:,('tweet_id','reply_count','retweet_count','favorite_count','quote_count')]
 
    dflist.append(df)

#Dataframe processed
dfwhole=pd.concat(dflist)
TD={}

for i in range(0,len(dfwhole.index)):
#Save the processed dataframe to pickle - folder name dfs
    row=dfwhole.iloc[i]
    tid=row['tweet_id']
    rep=row['reply_count']
    ret=row['retweet_count']
    fav=row['favorite_count']
    quo=row['quote_count']
    
    if tid not in TD:
        TD[tid]={}
    TD[tid]['reply']=rep
    TD[tid]['retweet']=ret
    TD[tid]['favorite']=fav
    TD[tid]['quote']=quo
       
path_dicts = ''
with open(datapath+path_dicts+'tweet'+db_name_table+tag+'.pkl', 'wb') as f:
    pickle.dump(TD, f, protocol=pickle.HIGHEST_PROTOCOL)

end = timer()
print(end - start)

print('Saved')

