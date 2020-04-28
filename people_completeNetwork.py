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
filtertag='_f'
doConnected=1

start=timer()

path_graphs = 'People/' 
with open(datapath+path_graphs+db_name_table+'NetPeople'+tag+filtertag+'.cnf', 'rb') as handle:
    Gu=pickle.load(handle)   

with open(datapath+path_graphs+db_name_table+'NetDPeople'+tag+filtertag+'.cnf', 'rb') as handle:
    G=pickle.load(handle) 

with open(datapath+path_graphs+db2+'People'+tag+'.cnf', 'rb') as handle:
    People=pickle.load(handle)

if Gu.has_node('None'):
    Gu.remove_node('None')
if G.has_node('None'):
    G.remove_node('None')

print(len(Gu.nodes))
print(len(G.nodes))
print(len(People))

nx.set_node_attributes(G, People)
nx.set_node_attributes(Gu, People)

nus=list(Gu.nodes())
print(nus[0])

nss=list(G.nodes())
print(nss[0])

for n in nus:

    succ=Gu.neighbors(n)
    for ns in succ:
        Gu[n][ns]['inv_weight']=1/(Gu[n][ns]['weight'])
        
for n in nss:

    succ=G.successors(n)
    #lensuc=len(succ)
    pred=G.predecessors(n)
    #lenpred=len(pred)
    
    for ns in succ:
        G[n][ns]['inv_weight']=1/(G[n][ns]['weight'])
    
    for np in pred:
        G[np][n]['inv_weight']=1/(G[np][n]['weight'])
        


print(list(People.keys())[0])

#print(G.nodes.data()[ns[0]]['followers'])
#for p in People:
#    if p in nus:
#        Gu.nodes[p]['followers']=People[p]['followers']
#        Gu.nodes[p]['following']=People[p]['following']
#        Gu.nodes[p]['favorites']=People[p]['favorites']
#    if p in ns:
#        G.nodes[p]['followers']=People[p]['followers']
#        G.nodes[p]['following']=People[p]['following']
#        G.nodes[p]['favorites']=People[p]['favorites']
my_ns2=G.nodes  
print(len(my_ns2))  
my_ns2copy=list(my_ns2).copy()   

if nx.is_weakly_connected(G):
        print('weakly connected')
else:
        print('unconnected')
        if doConnected:
            largest_cc = max(nx.weakly_connected_components(G), key=len)
            #print(largest_cc)
            print(len(largest_cc))
            for n2 in my_ns2copy:
                if not n2 in largest_cc:
                    G.remove_node(n2)
            if nx.is_weakly_connected(G):
                print('weakly connected')
            else:
                print('unconnected')        
            my_ns3=G.nodes  
            print(len(my_ns3))
            
            
my_ns2=Gu.nodes  
print(len(my_ns2))            
my_ns2copy=list(my_ns2).copy()
    
if nx.is_connected(Gu):
        print('connected')
else:
        print('unconnected')
        if doConnected:
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
                        

path_graphs = 'People/'  
#nx.write_gexf(Gu, datapath+path_graphs+db_name_table+'NetworkGraphPeople'+tag+'_c.gexf')
    
with open(datapath+path_graphs+db_name_table+'NetPeople'+tag+filtertag+'_c.cnf', 'wb') as handle:
    pickle.dump(Gu, handle, protocol=pickle.HIGHEST_PROTOCOL) 
    
#nx.write_gexf(G, datapath+path_graphs+db_name_table+'NetworkGraphDPeople'+tag+'_c.gexf')
    
with open(datapath+path_graphs+db_name_table+'NetDPeople'+tag+filtertag+'_c.cnf', 'wb') as handle:
    pickle.dump(G, handle, protocol=pickle.HIGHEST_PROTOCOL) 