from ast import Return
from functools import reduce
from turtle import update
import xml.etree.ElementTree as ET
from collections import Counter
import math
import re
import os
import pandas as pd
from collections import defaultdict


'''
    Paths
'''
# Absolute
path_abosolute = os.path.abspath('')
# Path output directory
path_output = os.path.join(path_abosolute, "output")
# Path Dataset (post.xml)
path_dataset = os.path.join(path_abosolute, "dataset/112010 Meta Stack Overflow/posts.xml")

print (path_abosolute)
print (path_output)
print (path_dataset)



def chunkify(iterable,len_of_chunk):
    for i in range(0,len(iterable), len_of_chunk):
        yield iterable[i:i + len_of_chunk]

def calcula_promedio(data1, data2):
    d = {} 
    for key,value in data2:
        try:
            d[key].append(value)
        except KeyError:
            d[key] = [value]
    data2=list(d.items())

    for dat in data2:
        data1.append(dat)

    d = {}
    for key,value in data1:
        try:
            for ab in value:
                d[key][0].append(ab)
        except KeyError:
            d[key] = [value]
    data1=list(d.items())

    datax=[]

    for ab in data1:
        tmp=[]

        for az in ab[1][0]:
            tmp.append(int(az))
        datax.append((ab[0],tmp))

    return datax
      

def mapper(data):
    relacion = list(map(obtiene_FavoriteCount_Score, data))
    relacion = list(filter(None, relacion))

    return relacion

def obtiene_FavoriteCount_Score(data):
    """
    Obtengo [FavoriteCount:Score]
    """
    post_type = data.attrib.get("PostTypeId")
    # Si el tipo del post no es pregunta, lo ignoro
    if post_type != "1":
        return
    else:
        try:

            FavoriteCount = data.attrib.get("FavoriteCount")
            Score = data.attrib.get("Score") 
        except Exception as e:
            print(f"Error: {e}")
        else:
            if str(FavoriteCount) != 'None':
                return (FavoriteCount, Score)

'''
1.- Busca todos los post, que no tiene respuesta aceptada
2.- Obtiene los FavoriteCount y  Score
3.- Calcula el promedio de Score por cada FavoriteCount 
4.- Muestra los  top 10 de FavoriteCount
'''

''' Lee el DATASET
'''
tree = ET.parse(path_dataset)
root = tree.getroot()
data_chunks = chunkify(root, 50)

# Map
mapped = list(map(mapper, data_chunks))

mapped = list(filter(None, mapped))
# Reduce
data1=mapped[0]

d = {} 

for key,value in data1:
    try:
        d[key].append(value)
    except KeyError:
        d[key] = [value]
d=list(d.items())

mapped[0]=d

# 'Reduce'
reduced = reduce(calcula_promedio, mapped)


# Dic: FavoriteCount, Score;
promedio={}
for ax in reduced:
    suma= sum(ax[1])
    promedio[ax[0]]= suma/len(ax[1])

top_10_FavoriteCount= dict(Counter(promedio).most_common(10))

print ('Top 10 \n FavoriteCount: Promedio')
for key,value in top_10_FavoriteCount.items():
    print (key ,value)

