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

print('Table loaded')
start=timer()

f=[]
fin=[]
fout=[]
for w in dict_distFreqPost:
    f.append(dict_distFreqPost[w])
    if w in keywords_list:
        fin.append(dict_distFreqPost[w])
    else:
        fout.append(dict_distFreqPost[w])

Gu=nx.Graph()

th=np.percentile(f,95)
print(th)
#ITERATE ON POSTS
   #ITERATE ON WORDS AFTER FILTERING
#Iterar tokens
for index, row in dfProcessed.iterrows():
    
    tokens_list = row['tokens_text']  
        
    for keyword in keywords_list:
        if keyword in tokens_list:            
            fk=dict_distFreqPost.get(keyword, 1)
            
            for relatedword in tokens_list:
               
                if relatedword != keyword:
                    f=dict_distFreqPost.get(relatedword, 1)
                    
                    if doFilter and f>=th:
                   
                        if not Gu.has_node(keyword):
                            Gu.add_node(keyword, freq = fk)
                        if not Gu.has_node(relatedword):
                            Gu.add_node(relatedword, freq = f)
                        if not Gu.has_edge(keyword,relatedword):  
                            Gu.add_edge(keyword,relatedword)  
                            #Flow
                            Gu[keyword][relatedword]['weight']=1
                     
                        else:
                            Gu[keyword][relatedword]['weight']=Gu[keyword][relatedword]['weight']+1
                    
                    if not doFilter:
                        print('sin filtro')
                        
                        if not Gu.has_node(keyword):
                            Gu.add_node(keyword, freq = fk)
                        if not Gu.has_node(relatedword):
                            Gu.add_node(relatedword, freq = f)
                        if not Gu.has_edge(keyword,relatedword):  
                            Gu.add_edge(keyword,relatedword)  
                            #Flow
                            Gu[keyword][relatedword]['weight']=1
                     
                        else:
                            Gu[keyword][relatedword]['weight']=Gu[keyword][relatedword]['weight']+1
       
end = timer()
print(end - start)

print('Finished. Writing to file')
       
path_graphs = 'Tweets/'  
nx.write_gexf(Gu, datapath+path_graphs+db_name_table+'NetworkGraph'+tagfile+filtertag+'.gexf')
    
with open(datapath+path_graphs+db_name_table+'Net'+tagfile+filtertag+'.cnf', 'wb') as handle:
    pickle.dump(Gu, handle, protocol=pickle.HIGHEST_PROTOCOL) 
    
print('Saved')
