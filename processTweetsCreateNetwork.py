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
db_name_table = str(argv[1])

print(type(str (db_name_table)))

start=timer()

# ====== Connection ====== #
# Connecting to mysql by providing a sqlachemy engine

#SQLAlchemy URI looks like this : 'mysql+mysqlconnector://user:password@host_ip:port/database'

#------mysql carlota--------
#Mysql@127.0.0.1:3306
#root
#password

#------mysql carlota--------

engine = create_engine('mysql+mysqlconnector://root:password@127.0.0.1:3306/twitterdb2',pool_recycle=3600)

#Reading database table to a dataframe
query = 'SELECT * FROM '+ db_name_table
data = pd.read_sql(query, engine)

# ====== PROCESS THE DATA ====== #

#Campos tweet_id,text,hashtags
df = data.loc[:,('tweet_id','text','hashtags')]

#sanity check, let’s look at the length of the string in text column in each entry.
df['pre_clean_len'] = [len(t) for t in df['text']]


#Funcion para eliminar puntuacion y numeros 
def remove_punct(text):
    
    punt = list(string.punctuation)#string.puntuation es un string - necesario convertirlo a lista
    punt.extend(['¿', '¡','’', '”', '“','•']) #añadir puntuacion española 
    text  = "".join([char for char in text if char not in punt])
    text = re.sub('[0-9]+', '', text) #numeros ??????
    return text

#lista con nombres propios en español - eliminar en preprocess

df_fnames = pd.read_json('https://query.data.world/s/rr6djouhowpilvpxxqzbeyjroemoyp')
df_mnames = pd.read_json('https://query.data.world/s/5elfg6gzndy3qcsepzwxotmik7tvbm')
female_names = df_fnames['name'].to_list()
male_names = df_mnames['name'].to_list()

spanish_names = male_names+female_names
spanish_names = list(map(lambda x:x.lower(),spanish_names))

#Funcion limpiar el texto
def clean_text(text):
    #---------Emoji patterns---------
    emoji_pattern = re.compile("["
         u"\U0001F600-\U0001F64F"  # emoticons
         u"\U0001F300-\U0001F5FF"  # symbols & pictographs
         u"\U0001F680-\U0001F6FF"  # transport & map symbols
         u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
         u"\U00002702-\U000027B0"
         u"\U000024C2-\U0001F251"
         "]+", flags=re.UNICODE)
    text_lw = text.lower()
        
    text_m = re.sub(r'@[A-Za-z0-9]+','',text_lw) #Remove mentions (is already stored in mentions)
    text_url = re.sub('https?://[A-Za-z0-9./]+','',text_m) #Remove urls
    text_e = re.sub (emoji_pattern, '',text_url ) #Remove emojis 
    text_ml = (BeautifulSoup(text_e, 'lxml').get_text())
    text_p = remove_punct(text_ml) #Remove Punt
    #Removing ...
    re1 = r"…[\s]"
    re2 = r"…$"
    text_pp = re.sub(re2,'', re.sub (re1,' ', text_p))
    tokens = nltk.word_tokenize(text_pp) #Tokeniz
    
    #Removing stop words in spanish 
    #Carlota: Añadido rt (aparece al comienzo de cada retweet)
    stopwords_spanish = stopwords.words('spanish')
    stopwords_spanish.extend(('rt','\u200d','https'))
    stopwords_spanish.extend((list(string.ascii_lowercase))) #Añade el abecedario
    
    tokens_stpw = list(filter(lambda x: x not in stopwords_spanish, tokens))
    
    return tokens_stpw

#Crea una columna en el dataframe aplicando la funcion clean_text()
df['tokens_text'] = df['text'].apply(lambda x: clean_text(x))

#Dataframe processed
dfProcessed = df.loc[:,('tweet_id','tokens_text')]

#Save the processed dataframe to pickle - folder name dfs
path_dfs = './pkls/dfs/'
dfProcessed.to_pickle(path_dfs+ db_name_table+'Processed.pkl')

#List of lists including all words --> frequency distribution
corpus_Post = list(itertools.chain.from_iterable(df['tokens_text'].to_list()))

#creacion del objeto FreqDist para el corpus construido
distFreq = nltk.FreqDist(corpus_Post)

#Convertirlo en dictionary para llamarlo en la creacion del grafo
dict_distFreqPost = dict(distFreq)

#Save to pickle en una carpeta que contiene los pkls (dictionaries)
path_dicts = './pkls/dicts/'
with open(path_dicts+'distFreq'+db_name_table+'.pkl', 'wb') as f:
    pickle.dump(dict_distFreqPost, f)
    

# ====== BUILD THE GRAPH ====== #

Gu=nx.Graph()

#Iterar tokens
for index, row in dfProcessed.iterrows():
    
    tokens_list = row['tokens_text']

    #Lista con las palabras clave definidas    
    keywords_list = ['ODS', 'sostenibilidad', 'desarrollo', 'sostenible', 'cooperación']
        
    for kw in keywords_list:
        if kw in tokens_list:
            keyword = kw
            
            for word in tokens_list:
                
                if word != keyword:
                    relatedword = word

                    #
                    if not Gu.has_node(keyword):
                        Gu.add_node(keyword, freq = dict_distFreqPost.get(keyword, 1))
                    if not Gu.has_node(relatedword):
                        Gu.add_node(relatedword, freq = dict_distFreqPost.get(relatedword, 1))
                    if not Gu.has_edge(keyword,relatedword):  
                        Gu.add_edge(keyword,relatedword)  
                        #Flow
                        Gu[keyword][relatedword]['weight']=1
                 
                    else:
                        Gu[keyword][relatedword]['weight']=Gu[keyword][relatedword]['weight']+1

    #Guarda los networks graphs en una carpeta llamada graphs
    path_graphs = './graphs/'  
    nx.write_gexf(Gu, path_graphs+db_name_table+'NetworkGraph.gexf')
    

end = timer()
print(end - start)

print('Finalizado OK')
