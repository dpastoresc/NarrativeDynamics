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
tag=''
doConnected=1
tagfile='3'
filtertag='_f'

start=timer()

path_graphs = 'Tweets/' 
with open(datapath+path_graphs+db_name_table+'Net'+tagfile+filtertag+'.cnf', 'rb') as handle:
    Gu=pickle.load(handle)   

if Gu.has_node('None'):
    Gu.remove_node('None')

nus=list(Gu.nodes())
print(nus[0])

for n in nus:

    succ=Gu.neighbors(n)
    for ns in succ:
        Gu[n][ns]['inv_weight']=1/(Gu[n][ns]['weight'])
        
my_ns2=Gu.nodes  
print(len(my_ns2))            
my_ns2copy=list(my_ns2).copy()
    
if nx.is_connected(Gu):
        print('connected')
else:
        print('unconnected')
        if do_connected:
            largest_cc = max(nx.connected_components(Gu), key=len)
            #print(largest_cc)
            print(len(largest_cc))
            for n2 in my_ns2copy:
                if not n2 in largest_cc:
                    Gu.remove_node(n2)
            if nx.is_connected(Gu):
                print('connected')
            else:
                print('unconnected')        
            my_ns3=Gu.nodes  
            print(len(my_ns3))
                        
path_graphs = 'Tweets/'  
with open(datapath+path_graphs+db_name_table+'Net'+tagfile+filtertag+'_c.cnf', 'wb') as handle:
    pickle.dump(Gu, handle, protocol=pickle.HIGHEST_PROTOCOL) 
    