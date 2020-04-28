# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np
import json
from nltk.tokenize import word_tokenize
import re
import geopandas as gpd
import networkx as nx
import matplotlib.pyplot as plt
import collections
from sklearn.cluster import KMeans
from PIL import Image
import time
from datetime import datetime, timedelta, date
from os import listdir
from os.path import isfile, join
from geopandas import GeoDataFrame
from shapely.geometry import Point
import pickle
import math
from timeit import default_timer as timer

#Se pasa como argumento el nombre de la tabla de la base de datos a procesar
doFilter=True
filtertag='_f'
tagfile="3"
db_name_table = 'PostsMadCar'#str(argv[1])
datapath='/home/davidpastor/Narrativas/MadCar/'
m_database='twitterdb'

#Lista con las palabras clave definidas    
keywords_list = ['descarbonización','descarbonizacion','clima','climático','climatico','combustible', 'CO2', 'climática', 'climatica', 'transición energética', 'renovable', 'energía', 'energia', 'energético', 'energética', 'energetico', 'energetica']

m_user='david'
m_pass='password'
address='192.168.0.154'
address='127.0.0.1:3306'
encoding = 'utf-8'


path_dicts = 'Tweets/'
with open(datapath+path_dicts+'distFreq'+db_name_table+tagfile+'.pkl', 'rb') as f:
   dict_distFreqPost = pickle.load(f)
   
path_dfs = 'Tweets/'
dfProcessed = pd.read_pickle(datapath+path_dfs+ db_name_table+'Processed'+tagfile+'.pkl') 

path_graphs = 'Tweets/'
with open(datapath+path_graphs+db_name_table+'Descriptors'+tagfile+filtertag+'.cnf', 'rb') as handle:
   GD= pickle.load(handle)

print('Table loaded')

start=timer()

#ITERATE ON POSTS
   #ITERATE ON WORDS AFTER FILTERING
#Iterar tokens
desc= ['cfbetweenness', 'betweenness', 'closennes', 'cfcloseness', 'eigenvalue', 'degree', 'load']

scores={}
for index, row in dfProcessed.iterrows():
    
    tweetid=row['tweetid']
    tokens_list = row['tokens_text']
    
    if tweetid not in scores:
        scores[tweetid]={}
        for d1 in desc:
            scores[tweetid][d1]=0
            scores[tweetid]['freq']=0
        
    for word in tokens_list:
        
        fk=dict_distFreqPost.get(word, 1)
        if word in GD:
            for d in GD[word]:
                scores[tweetid][d]=scores[tweetid][d]+GD[word]
            scores[tweetid]["freq"]=scores[tweetid]["freq"]+fk

       
end = timer()
print(end - start)

print('Finished. Writing to file')
       
path_graphs = 'Tweets/'  
    
with open(datapath+path_graphs+db_name_table+'Scores'+tagfile+filtertag+'.cnf', 'wb') as handle:
    pickle.dump(scores, handle, protocol=pickle.HIGHEST_PROTOCOL) 
    
print('Saved')
