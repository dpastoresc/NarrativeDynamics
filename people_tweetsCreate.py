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
db_name_table = 'UsersMadCar'#str(argv[1])
datapath='/home/davidpastor/Narrativas/MadCar/'
keywords_list = ['descarbonización','descarbonizacion','clima','climático','climatico','combustible', 'CO2', 'climática', 'climatica', 'transición energética', 'renovable', 'energía', 'energia', 'energético', 'energética', 'energetico', 'energetica']

m_user='david'
m_pass='password'
m_database='twitterdb'
address='192.168.0.154'
address='127.0.0.1:3306'
encoding = 'utf-8'

print(type(str (db_name_table)))
start=timer()

engine = create_engine('mysql+mysqlconnector://'+m_user+":"+m_pass+'@'+address+'/'+m_database,pool_recycle=3600)

#Reading database table to a dataframe
query = 'SELECT * FROM '+ db_name_table
data = pd.read_sql(query, engine)

# ====== PROCESS THE DATA ====== #

#Campos tweet_id,text,hashtags
df = data.loc[:,('user_follow_count','user_friends_count','user_fav_count','user_id')]


G=nx.DiGraph()
Gu=nx.Graph()
People={}
#Iterar tokens
for index, row in df.iterrows():
    
    uid = row['user_id']
    fol=row['user_follow_count']
    friend=row['user_friends_count']
    fav=row['user_fav_count']
    
    #print(uid)
    #print(fol)
    #print(friend)
    #print(fav)

    if uid not in People:
        People[uid]={}
    People[uid]['followers']=fol
    People[uid]['following']=friend
    People[uid]['favorites']=fav

with open(datapath+db_name_table+'People.cnf', 'wb') as handle:
    pickle.dump(People, handle, protocol=pickle.HIGHEST_PROTOCOL) 