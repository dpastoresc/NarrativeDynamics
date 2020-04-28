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

#Funcion para eliminar puntuacion y numeros 
def remove_punct(text):
    
    punt = list(string.punctuation)#string.puntuation es un string - necesario convertirlo a lista
    punt.extend(['¿', '¡','’', '”', '“','•']) #añadir puntuacion española 
    text  = "".join([char for char in text if char not in punt])
    text = re.sub('[0-9]+', '', text) #numeros ??????
    
    #Delete puntos suspensivos al final de palabras
    re11 = r'(…){1,5}'
       
    text = re.sub(re11,'',text)
    text = text.lower()
    return text

#Funcion que normaliza la risa
def normalize_laughs(message):
    message = re.sub(r'\b(?=\w*[j])[aeiouj]{3,}\b', 'risa', message, flags=re.IGNORECASE)
    message = re.sub(r'\b(juas+|lol)\b', 'risa', message, flags=re.IGNORECASE)
    return message

def norm_jerga(text):
#Normalizacion de la jerga
    jerga = [
        ('d','de')
        ,('[qk]','que')
        ,('xo','pero') 
        ,('xa', 'para') 
        ,('[xp]q','porque')
        ,('es[qk]', 'es que')
        ,('fvr','favor')
        ,('(xfa|xf|pf|plis|pls|porfa|xfi|porfi)', 'por favor')
        ,('dnd','donde')
        ,('tb', 'también')
        ,('(tq|tk)', 'te quiero')
        ,('(tqm|tkm)', 'te quiero mucho')
        ,('x','por')
        ,('\+','mas')
        ,('akba', 'acaba')
        ,('a2', 'adiós')
        ,('aa', 'años')
        ,('archvo', 'archivo')
        ,('artclo', 'artículo')
        ,('artfcial', 'artificial')
        ,('ataq', 'ataque')
        ,('atrizar', 'aterrizar')
        ,('ad+', 'además')
        ,('bte|bstnt', 'bastante')
        ,('batry', 'batería')
        ,('bbr', 'beber')
        ,('bn', 'bien')
        ,('bra', 'brasil')
        ,('brma', 'broma')
        ,('brmar', 'bromear')
        ,('botya', 'botella')
        ,('brbja', 'burbuja')
        ,('csa|ksa', 'casa')
        ,('zibrar', 'celebrar')
        ,('cel', 'celular')
        ,('zntral|cntral', 'central')
        ,('zntro', 'centro')
        ,('xat', 'chat')
        ,('xatr', 'chatear')
        ,('cmnkt', 'comunícate')
        ,('qal', 'cuál')
        ,('qalkera', 'cualquiera')
        ,('cdo|qndo|cndo', 'cuando')
        ,('cto', 'cuanto')
        ,('cn', 'con')
        ,('d', 'de')
        ,('dbria', 'debería')
        ,('d+', 'demás')
        ,('dcir', 'decir')
        ,('dicnario', 'diccionario')
        ,('dir', 'dirección')
        ,('dxo|dixo', 'dicho')
        ,('dnd|dd|dnde', 'donde')
        ,('qmple', 'cumpleaños')
        ,('ej', 'ejemplo')
        ,('emrgencia', 'emergencia')
        ,('empzar', 'empezar')
        ,('enqntro', 'encuentro')
        ,('entrda', 'entrada')
        ,('skpar', 'escapar')
        ,('sprar', 'esperar')
        ,('stdo', 'estado')
        ,('est', 'éste')
        ,('exam', 'examen')
        ,('xclnt', 'excelente')
        ,('esq', 'es que')
        ,('exo', 'hecho')
        ,('exado', 'echado')
        ,('flidads', 'felicidades')
        ,('fsta', 'fiesta')
        ,('firm', 'firme')
        ,('frt', 'fuerte')
        ,('gnrcn', 'generación')
        ,('gral', 'general')
        ,('gnt', 'gente')
        ,('grdo', 'gordo')
        ,('thanks|thanx|graxias|tks', 'gracias')
        ,('gdo', 'grado')
        ,('grduar', 'graduar')
        ,('acer', 'hacer')
        ,('azla', 'hazla')
        ,('hno', 'hermano')
        ,('hr', 'hora')
        ,('ncrible', 'increíble')
        ,('indval', 'individual')
        ,('info', 'información')
        ,('infrmal', 'informal')
        ,('ntimo', 'íntimo')
        ,('jf', 'jefe')
        ,('jnts', 'juntos')
        ,('jvnil', 'juvenil')
        ,('kg', 'kilogramo')
        ,('km', 'kilómetro')
        ,('llmame', 'llámame')
        ,('yav', 'llave')
        ,('lgr', 'lugar')
        ,('lu', 'lunes')
        ,('mñna', 'mañana')
        ,('mjr', 'mejor')
        ,('msj', 'mensaje')
        ,('mntir', 'mentir')
        ,('mxo|muxo', 'mucho')
        ,('vlor', 'valor')
        ,('vmos', 'vamos')
        ,('vcino', 'vecino')
        ,('vstir', 'vestir')
        ,('vje', 'viaje')
        ,('vistzo', 'vistazo')
        ,('mens|msj', 'mensaje')
        ,('mto', 'moto')
        ,('mv|mov', 'móvil')
        ,('mxo|muxo', 'mucho')
        ,('nl', 'en el')
        ,('olvdr', 'olvidar')
        ,('oprcn', 'operación')
        ,('pal', 'para el')
        ,('pco', 'poco')
        ,('pkñ', 'pequeño')
        ,('pqñ', 'pequeño')
        ,('pso', 'paso')
        ,('pdo', 'borrachera')
        ,('q', 'que')
        ,('qn', 'quien')
        ,('rmno', 'hermano')
        ,('rptlo', 'repítelo')
        ,('sq|esq', 'es que')
        ,('salu2', 'saludos')
        ,('sbdo', 'sábado')
        ,('sbs', 'sabes')
        ,('slmos', 'salimos')
        ,('spro', 'espero')
        ,('srt', 'suerte')
        ,('tas', 'estás')
        ,('tb', 'también')
        ,('tbj|trbjo', 'trabajo')
        ,('tjt', 'tarjeta')
        ,('tel|telfono', 'teléfono')
        ,('tv', 'televisión')
        ,('tng|tngo', 'tengo')
        ,('trd|trde', 'tarde')
        ,('vac|vacas', 'vacaciones')
        ,('vns', 'vienes')
        ,('vos', 'vosotros')
        ,('vrns', 'viernes')
        ,('vnir', 'venir')
        ,('verdd', 'verdad')
        ,('wpa|wapa', 'guapa')
        ,('xcierto', 'por cierto')
        ,('xdon', 'perdón')
        ,('xka', 'chica')
        ,('xko', 'chico')
        ,('xo', 'pero')
        ,('ymam', 'llámame') 
        ,('stoy', 'estoy')
        ,('kdarme','quedarme')
        ,('n','en')
    ]
    for s,t in jerga:
        text = re.sub(r'\b{0}\b'.format(s), t, text)
    return text

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
    
    text_lw_pre = text.lower()
    text_lw=text_lw_pre.decode(encoding)
    
    text_m = re.sub(r'@[A-Za-z0-9]+','',text_lw) #Remove mentions (is already stored in mentions)
    text_url = re.sub('https?://[A-Za-z0-9./]+','',text_m) #Remove urls
    text_e = re.sub (emoji_pattern, '',text_url ) #Remove emojis 
    text_ml = (BeautifulSoup(text_e, 'lxml').get_text())
    text_j = norm_jerga(text_ml)#Normalizar jerga internet
    
    text_r = re.sub(r'(.)\1{2,}', r'\1\1', text_j) #Remove repeated characters
    
    text_p = remove_punct(text_r) #Remove Punt
    #Removing ...
    re1 = r"…[\s]"
    re2 = r"…$"
    text_pp = re.sub(re2,'', re.sub (re1,' ', text_p))
    text_til = text_pp#quitar_tildes(text_pp)
    text_l = normalize_laughs(text_til)
    tokens = nltk.word_tokenize(text_l) #Tokeniz
    
    #Removing stop words in spanish 
    #Carlota: Añadido rt (aparece al comienzo de cada retweet)
    stopwords_spanish = stopwords.words('spanish')
    stopwords_spanish.extend(('rt','\u200d','https'))
    stopwords_spanish.extend((list(string.ascii_lowercase))) #Añade el abecedario
    
    tokens_stpw = list(filter(lambda x: x not in stopwords_spanish, tokens))
    tokens_stpw_names = list(filter(lambda x: x not in spanish_names, tokens_stpw))
    
    return tokens_stpw_names
#lista con nombres propios en español - eliminar en preprocess

df_fnames = pd.read_json('https://query.data.world/s/rr6djouhowpilvpxxqzbeyjroemoyp')
df_mnames = pd.read_json('https://query.data.world/s/5elfg6gzndy3qcsepzwxotmik7tvbm')
female_names = df_fnames['name'].to_list()
male_names = df_mnames['name'].to_list()

spanish_names = male_names+female_names
spanish_names = list(map(lambda x:x.lower(),spanish_names))

#Se pasa como argumento el nombre de la tabla de la base de datos a procesar
db_name_table = 'PostsCorMad'#str(argv[1])
m_user='david'
m_pass='password'
m_database='TwitterDisaster'
datapath='/home/davidpastor/Narrativas/CorMad/'
address='192.168.0.154'
address='127.0.0.1:3306'
encoding = 'utf-8'

#Lista con las palabras clave definidas    

keywords_list = ['descarbonización','descarbonizacion','clima','climático','climatico','combustible', 'CO2', 'climática', 'climatica', 'transición energética', 'renovable', 'energía', 'energia', 'energético', 'energética', 'energetico', 'energetica']
keywords_list = ['coronavirus', 'Coronavirus', '#CoronavirusES', 'coronavirusESP', '#coronavirus', '#Coronavirus','covid19', '#covid19','Covid19', '#Covid19', 'covid-19', '#covid-19', 'COVID-19', '#COVID-19']

#keywords_list = ['ODS', 'sostenibilidad', 'desarrollo', 'sostenible', 'cooperación']

print(type(str (db_name_table)))
start=timer()

engine = create_engine('mysql+mysqlconnector://'+m_user+":"+m_pass+'@'+address+'/'+m_database,pool_recycle=3600)

#Reading database table to a dataframe

query = 'SELECT COUNT(*) FROM '+ db_name_table
data = pd.read_sql(query, engine)
nrows=data["COUNT(*)"][0]

dflist=[]

for i in range(0, nrows, 1000):
    data  = pd.read_sql("SELECT * FROM "+db_name_table+ " LIMIT "+str(i)+",1000", engine)

    df = data.loc[:,('tweet_id','text','hashtags')]
    
    #sanity check, let’s look at the length of the string in text column in each entry.
    df['pre_clean_len'] = [len(t) for t in df['text']]

    #Funcion limpiar el texto
    
    #Crea una columna en el dataframe aplicando la funcion clean_text()
    df['tokens_text'] = df['text'].apply(lambda x: clean_text(x))
    dflist.append(df)

#Dataframe processed
dfwhole=pd.concat(dflist)
dfProcessed = dfwhole.loc[:,('tweet_id','tokens_text')]


#Save the processed dataframe to pickle - folder name dfs
path_dfs = 'pkls/dfs/'
dfProcessed.to_pickle(datapath+path_dfs+ db_name_table+'Processed.pkl')

#List of lists including all words --> frequency distribution
corpus_Post = list(itertools.chain.from_iterable(df['tokens_text'].to_list()))

#creacion del objeto FreqDist para el corpus construido
distFreq = nltk.FreqDist(corpus_Post)

#Convertirlo en dictionary para llamarlo en la creacion del grafo
dict_distFreqPost = dict(distFreq)

#Save to pickle en una carpeta que contiene los pkls (dictionaries)
path_dicts = 'pkls/dicts/'
with open(datapath+path_dicts+'distFreq'+db_name_table+'.pkl', 'wb') as f:
    pickle.dump(dict_distFreqPost, f)
    

# ====== BUILD THE GRAPH ====== #
print('Building graph')
Gu=nx.Graph()

#Iterar tokens
for index, row in dfProcessed.iterrows():
    
    tokens_list = row['tokens_text']
        
    for kw in keywords_list:
        if kw in tokens_list:
            keyword = kw
            
            for word in tokens_list:
                
                if word != keyword:
                    relatedword = word

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

print('Finalizado OK, saving')

path_graphs = ''  
nx.write_gexf(Gu, datapath+path_graphs+db_name_table+'NetworkGraph.gexf')
    
with open(datapath+path_graphs+db_name_table+'Net.cnf', 'wb') as handle:
    pickle.dump(Gu, handle, protocol=pickle.HIGHEST_PROTOCOL) 
    
end = timer()
print(end - start)

print('Saved')

