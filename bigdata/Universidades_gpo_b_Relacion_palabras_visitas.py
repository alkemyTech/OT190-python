from ast import Return
from functools import reduce
import xml.etree.ElementTree as ET
from collections import Counter
import math
import re
import os
import pandas as pd


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

def reducir_contadores(data1, data2):
    for key, value in data2.items():
        try:
            if key in data1.keys():
                data1.update({key: data1[key] + value})
            else:
                data1.update({key: value})
        except:
            return
    return data1

def mapper(data):
    relacion = list(map(palabras_visitas, data))
    relacion = list(filter(None, relacion))
    # try:
    #     reduced = reduce(relation_math, relacion)
    # except:
    #     return
    return relacion

def palabras_visitas(data):
    """
    Relacion palabras post - visitas
    """
    post_type = data.attrib.get("PostTypeId")
    # Si post es pregunta
    # Si el tipo del post no es pregunta, lo ignoro
    if post_type != "1":
        return
    else:
        try:
            visitas = int(data.attrib.get("ViewCount"))
            body = data.attrib.get("Body")
        except Exception as e:
            print(f"Error {e}")
        else:
            if not body:
                return
            else:
                body = re.findall( r"(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))", body )
                cantidas_palabras = len(body)
            try:
                if visitas != 0:
                    return {cantidas_palabras: visitas}
            except:
                return
            
'''

1.- Busca todos los post pregunta
2.- Obtiene los cantidad palabras vs visitas en los post
3.- Muestra pablas:visitas
4.- 
'''


''' Lee el DATASET
'''
tree = ET.parse(path_dataset)
root = tree.getroot()
data_chunks = chunkify(root, 50)

# Map
mapped = list(map(mapper, data_chunks))
mapped = list(filter(None, mapped))
print (mapped)

# Reduce
# reduced = reduce(calcular_relacion, mapped)
