#!/usr/bin/python3 

import numpy as np
import networkx as nx 
import sys 
import smart_open
from gensim.models import Word2Vec
import random 

EPS = 1e-15

EPOCHS=1
    
for line in sys.stdin:
    path = line.strip()
    walks = []
    for line in smart_open.open(path):
        if random.random() < 0.5:
            walks.append(line.strip().split())
    model = Word2Vec(sentences=walks, size=128, window=2, min_count=0, workers=12, iter=EPOCHS)

    for node in model.wv.index2entity:
        print("{} {}".format(node, " ".join(map(str,model.wv.get_vector(node).tolist()))))