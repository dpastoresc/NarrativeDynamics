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


db_name_table = 'PostsMadCar'#str(argv[1])
#db_name_table = 'PostsCorMad'#str(argv[1])
db2='UsersMadCar'#str(argv[1])
#db2='UsersCorMad'#str(argv[1])
datapath='/home/davidpastor/Narrativas/MadCar/'
#datapath='/home/davidpastor/Narrativas/CorMad/'
tagfile='3'
filtertag='_f'

start=timer()

GD = {}

path_graphs='Tweets/'

with open(datapath+path_graphs+db_name_table+'Net'+tagfile+filtertag+'_c.cnf', 'rb') as handle:
    Gu=pickle.load(handle)   

my_ns=Gu.nodes
print(len(my_ns))

if nx.is_connected(Gu):
    print('connected')
else:
    print('unconnected')
    
#Betweenness centrality
BFCN=nx.current_flow_betweenness_centrality(Gu, weight='inv_weight')
BCN=nx.betweenness_centrality(Gu, weight='inv_weight')
print('Done cf betweenness')

#Closeness centrality
CFCN=nx.current_flow_closeness_centrality(Gu,weight='inv_weight')
CCN=nx.closeness_centrality(Gu, distance='inv_weight')
print('Done cf closeness')

#PCN=nx.percolation_centrality(Gu, weight='inv_weight')

#Eigenvalue centrality
ECN=nx.eigenvector_centrality(Gu,max_iter=200,weight='weight')
print('Done eigenvalue')

DCN=nx.degree_centrality(Gu)
print('Done degree')

LCN=nx.load_centrality(Gu, weight='inv_weight')
print('Done load')

for n in my_ns:
    #print(n)
    if n not in GD:
        GD[n]={}
        
    GD[n]['cfbetweenness']=BFCN[n]
    GD[n]['betweenness']=BCN[n]  
    GD[n]['closeness']=CCN[n]
    GD[n]['cfcloseness']=CFCN[n]
    GD[n]['eigenvalue']=ECN[n]
    GD[n]['degree']=DCN[n]
    GD[n]['load']=LCN[n]  
    
with open(datapath+path_graphs+db_name_table+'Descriptors'+tagfile+filtertag+'.cnf', 'wb') as handle:
    pickle.dump(GD, handle, protocol=pickle.HIGHEST_PROTOCOL)

end = timer()
print(end - start)
