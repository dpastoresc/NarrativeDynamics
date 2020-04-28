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

db_name_table = 'UsersMadCar'#str(argv[1])
#db_name_table = 'UsersCorMad'#str(argv[1])
datapath='/home/davidpastor/Narrativas/MadCar/'
#datapath='/home/davidpastor/Narrativas/CorMad/'
m_database='twitterdb'
#m_database='TwitterDisaster'
tag=''

m_user='david'
m_pass='password'
address='192.168.0.154'
address='127.0.0.1:3306'
encoding = 'utf-8'

print(type(str (db_name_table)))
start=timer()

engine = create_engine('mysql+mysqlconnector://'+m_user+":"+m_pass+'@'+address+'/'+m_database,pool_recycle=3600)

query = 'SELECT COUNT(*) FROM '+ db_name_table
data = pd.read_sql(query, engine)
nrows=data["COUNT(*)"][0]

dflist=[]

for i in range(0, nrows, 10000):
    data  = pd.read_sql("SELECT * FROM "+db_name_table+ " LIMIT "+str(i)+",100", engine)

    df = data.loc[:,('user_follow_count','user_friends_count','user_fav_count','user_id')]
    dflist.append(df)
    
dfwhole=pd.concat(dflist)

G=nx.DiGraph()
Gu=nx.Graph()
People={}
#Iterar tokens
for index, row in dfwhole.iterrows():
    
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