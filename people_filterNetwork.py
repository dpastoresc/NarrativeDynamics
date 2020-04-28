# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np
import seaborn as sns

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


db_name_table = 'PostsMadCar'#str(argv[1])
#db_name_table = 'PostsCorMad'#str(argv[1])
db2='UsersMadCar'#str(argv[1])
#db2='UsersCorMad'#str(argv[1])
datapath='/home/davidpastor/Narrativas/MadCar/'
#datapath='/home/davidpastor/Narrativas/CorMad/'
tag=''
th=10

start=timer()

path_graphs = 'People/' 
with open(datapath+path_graphs+db_name_table+'NetPeople'+tag+'.cnf', 'rb') as handle:
    Gu=pickle.load(handle)   

with open(datapath+path_graphs+db_name_table+'NetDPeople'+tag+'.cnf', 'rb') as handle:
    G=pickle.load(handle) 

with open(datapath+path_graphs+db2+'People'+tag+'.cnf', 'rb') as handle:
    People=pickle.load(handle)

Gu.remove_node('None')
G.remove_node('None')

nu=Gu.nodes()
vu=[]
gudata=Gu.nodes.data()
for n in nu:
    vu.append(Gu.degree(n))
#    if 'followers' in gudata[n]:
#        print('hola')
   
print(len(vu))
vuc=[i for i in vu if i>10]
print(len(vuc))

sns.set_style('darkgrid')   
sns_plot = sns.distplot(vu)
sns_plot.figure.savefig("Gu_nodehist.png")

ns=G.nodes()
v=[]
gdata=G.nodes.data()
for n in ns:
    v.append(G.out_degree(n))
#    if 'followers' in gdata[n]:
#        print('hola')

print(len(v))
vc=[i for i in v if i>10]
print(len(vc))

v2=[]

for n in ns:
    v2.append(G.in_degree(n))

print(len(v2))
vc2=[i for i in v2 if i>10]
print(len(vc2))

sns.set_style('darkgrid')   
sns_plot = sns.distplot(v)
sns_plot.figure.savefig("G_nodehist.png")
    

Guf=Gu.copy()
nus=Gu.nodes()
for n in nus:
    dn=Gu.degree(n)
    if dn<th:
        Guf.remove_node(n)
        
Gf=G.copy()
ns=G.nodes()
for n in ns:
    dn=G.out_degree(n)
    if dn<th:
        Gf.remove_node(n)

print(len(Guf.nodes()))
print(len(Gf.nodes()))

path_graphs = 'People/'  
nx.write_gexf(Guf, datapath+path_graphs+db_name_table+'NetworkGraphPeople'+tag+'_f.gexf')
    
with open(datapath+path_graphs+db_name_table+'NetPeople'+tag+'_f.cnf', 'wb') as handle:
    pickle.dump(Guf, handle, protocol=pickle.HIGHEST_PROTOCOL) 
    
nx.write_gexf(Gf, datapath+path_graphs+db_name_table+'NetworkGraphDPeople'+tag+'_f.gexf')
    
with open(datapath+path_graphs+db_name_table+'NetDPeople'+tag+'_f.cnf', 'wb') as handle:
    pickle.dump(Gf, handle, protocol=pickle.HIGHEST_PROTOCOL) 