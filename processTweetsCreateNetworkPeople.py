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
db_name_table = 'PostsMadCar'#str(argv[1])
db_name_table = 'PostsCorMad'#str(argv[1])
datapath='/home/davidpastor/Narrativas/MadCar/'
datapath='/home/davidpastor/Narrativas/CorMad/'

keywords_list = ['descarbonización','descarbonizacion','clima','climático','climatico','combustible', 'CO2', 'climática', 'climatica', 'transición energética', 'renovable', 'energía', 'energia', 'energético', 'energética', 'energetico', 'energetica']


m_user='david'
m_pass='password'
m_database='twitterdb'
address='192.168.0.154'
address='127.0.0.1:3306'
encoding = 'utf-8'

#Lista con las palabras clave definidas    


#keywords_list = ['ODS', 'sostenibilidad', 'desarrollo', 'sostenible', 'cooperación']

print(type(str (db_name_table)))
start=timer()

engine = create_engine('mysql+mysqlconnector://'+m_user+":"+m_pass+'@'+address+'/'+m_database,pool_recycle=3600)

#Reading database table to a dataframe
query = 'SELECT * FROM '+ db_name_table
data = pd.read_sql(query, engine)

# ====== PROCESS THE DATA ====== #

#Campos tweet_id,text,hashtags
df = data.loc[:,('tweet_id','user_id','reply_user_id','retweet_user_id', 'quote_user_id', 'mention_ids')]


G=nx.DiGraph()
Gu=nx.Graph()
People={}
#Iterar tokens
for index, row in df.iterrows():
    
    uid=row['user_id']
    reply=row['reply_user_id']
    retweet=row['retweet_user_id']
    quote=row['quote_user_id']
    mention=row['mention_ids']
    
    if not Gu.has_node(uid):
        Gu.add_node(uid)
    if not G.has_node(uid):
        G.add_node(uid)   
    
    #REPLIES
    if not pd.isnull(reply):
        if not Gu.has_node(reply):
            Gu.add_node(reply)
        
        if not G.has_node(reply):
            G.add_node(reply)         
        
        if not Gu.has_edge(uid, reply):  
            Gu.add_edge(uid, reply)
            Gu[uid][reply]['weight']=1
        else:
            Gu[uid][reply]['weight']=Gu[uid][reply]['weight']+1
        
        if not G.has_edge(uid, reply):  
            G.add_edge(uid, reply)
            G[uid][reply]['weight']=1
        else:
            G[uid][reply]['weight']=G[uid][reply]['weight']+1    
        
    #RETWEETS
    if not pd.isnull(retweet):
        if not Gu.has_node(retweet):
            Gu.add_node(retweet)
        
        if not G.has_node(retweet):
            G.add_node(retweet)         
        
        if not Gu.has_edge(uid, retweet):  
            Gu.add_edge(uid, retweet)
            Gu[uid][retweet]['weight']=1
        else:
            Gu[uid][retweet]['weight']=Gu[uid][retweet]['weight']+1
        
        if not G.has_edge(uid, retweet):  
            G.add_edge(uid, retweet)
            G[uid][retweet]['weight']=1
        else:
            G[uid][retweet]['weight']=G[uid][retweet]['weight']+1
        
    #QUOTE
    if not pd.isnull(quote):
        if not Gu.has_node(quote):
            Gu.add_node(quote)
        
        if not G.has_node(quote):
            G.add_node(quote)         
        
        if not Gu.has_edge(uid, quote):  
            Gu.add_edge(uid, quote)
            Gu[uid][quote]['weight']=1
        else:
            Gu[uid][quote]['weight']=Gu[uid][quote]['weight']+1
        
        if not G.has_edge(uid, quote):  
            G.add_edge(uid, quote)
            G[uid][quote]['weight']=1
        else:
            G[uid][quote]['weight']=G[uid][quote]['weight']+1
        
    #MENNTION
    for m in mention:
        if not pd.isnull(m):
            if not Gu.has_node(m):
                Gu.add_node(m)
            
            if not G.has_node(quote):
                G.add_node(m)         
            
            if not Gu.has_edge(uid, m):  
                Gu.add_edge(uid, m)
                Gu[uid][m]['weight']=1
            else:
                Gu[uid][m]['weight']=Gu[uid][m]['weight']+1
            
            if not G.has_edge(uid, m):  
                G.add_edge(uid, m)
                G[uid][m]['weight']=1
            else:
                G[uid][m]['weight']=G[uid][m]['weight']+1
    
path_graphs = ''  
nx.write_gexf(Gu, datapath+path_graphs+db_name_table+'NetworkGraphPeople.gexf')
    
with open(datapath+path_graphs+db_name_table+'NetPeople.cnf', 'wb') as handle:
    pickle.dump(Gu, handle, protocol=pickle.HIGHEST_PROTOCOL) 
    
nx.write_gexf(G, datapath+path_graphs+db_name_table+'NetworkGraphDPeople.gexf')
    
with open(datapath+path_graphs+db_name_table+'NetDPeople.cnf', 'wb') as handle:
    pickle.dump(G, handle, protocol=pickle.HIGHEST_PROTOCOL) 
    
    
end = timer()
print(end - start)

print('Finalizado OK')
