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
place='Madrid'
tag=''

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
init=nrows-1500

dflist=[]

for i in range(init, nrows, 1000):
    data  = pd.read_sql("SELECT * FROM "+db_name_table+ " LIMIT "+str(i)+",1000", engine)

    df = data.loc[:,('tweet_id','place_id', 'place_name', 'coord')]
 
    dflist.append(df)

#Dataframe processed
dfwhole=pd.concat(dflist)
path_dfs = ''
dfwhole.to_pickle(datapath+path_dfs+ db_name_table+'_geo.pkl')

LD={}

for i in range(0,len(dfwhole.index)):
#Save the processed dataframe to pickle - folder name dfs
    row=dfwhole.iloc[i]
    tid=row['tweet_id']
    location=row['coord']
    place_id=row['place_id']
    place_name=row['place_name']
    if tid not in LD:
        LD[tid]=-1
    if location=='None':
        if not place_id=='None':
            if place_id==place:
                LD[tid]=1
            else:
                LD[tid]=0        
    else: 
        print(location)
        gps=location.split(';')
        lon=float(gps[0])
        lat=float(gps[1])
        if lon>=geo[0] and lon <= geo[2] and lat>=geo[1] and lat<=geo[3]:
            LD[tid]=1
        else:
            LD[tid]=0    
        print(location)
    
path_dicts = ''
with open(datapath+path_dicts+'geo'+db_name_table+tag+'.pkl', 'wb') as f:
    pickle.dump(LD, f, protocol=pickle.HIGHEST_PROTOCOL)
    
end = timer()
print(end - start)

print('Saved')

