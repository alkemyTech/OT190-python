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
    tags_aceptados = list(map(top_tags_en_preguntas_no_contestadas, data))
    # Filtra los datos, quitando aquellos donde la Key es NONE
    tags_aceptados = list(filter(None, tags_aceptados))
    # Por cada chunk, agrega los tags a una lista, para despues contarlos
    try:
        # Counter(x for xs in seq for x in set(xs))
        data_process = [tag for sublist in tags_aceptados for tag in sublist]
    except:
        return
    apx = dict(Counter(data_process))
    return apx


def top_tags_en_preguntas_no_contestadas(data):
    post_type = data.attrib.get("PostTypeId")
    # Solo revisar las POST preguntas, pues el único que tiene TAGS
    if post_type == "1":
        try:
            # Busca obtener el AcceptedAnswerId, que indica si se tomo alguna respuesta como ACEPTADA
            accepted_id = data.attrib.get("AcceptedAnswerId")
            # Obtiene los tags
            tags = data.attrib.get("Tags")

        except:
            return
        tag_list= re.findall('<(.+?)>',tags)
        #Agrega los tags si el post no esta como aceptado
        if not accepted_id:

            return tag_list
    return

'''

1.- Busca todos los post, que no tiene respuesta aceptada
2.- Obtiene los tags en esos post
3.- Cuenta cuantas veces se repite cada tag
4.- Muestra los 10 tags más repetidos

'''

''' Lee el DATASET
'''
tree = ET.parse(path_dataset)
root = tree.getroot()
data_chunks = chunkify(root, 50)

# Map
mapped = list(map(mapper, data_chunks))

# Reduce
reduced = reduce(reducir_contadores, mapped)

top_10_tags= dict(Counter(reduced).most_common(10))
print (top_10_tags)