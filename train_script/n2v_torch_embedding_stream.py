#!/usr/bin/python3 

import torch
from torch_geometric.data import Data
from torch_geometric.nn import Node2Vec
import numpy as np
import networkx as nx 
import sys 
import smart_open

EPS = 1e-15

EPOCHS=0
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
# device = torch.device('cpu')
    
for line in sys.stdin:
    path = line.strip()
    node2id = dict()
    edge_list = set()

    for line in smart_open.open(path):
        try:
            node1, node2  = list(map(int, line.strip().split()))

            try:
                id1 = node2id[node1]
            except:
                id1 = len(node2id)
                node2id[node1] = id1

            try:
                id2 = node2id[node2]
            except:
                id2 = len(node2id)
                node2id[node2] = id2
            
            edge_list.add((id1, id2))
            # edge_list.add((id2, id1))
        except:
            pass

    edge_list = list(edge_list)
    if len(node2id) == 0:
        print("")
    else:

        edge_index = torch.tensor(np.array(edge_list).T, dtype=torch.long)

        data = Data(edge_index=edge_index)

        model = Node2Vec(data.edge_index, embedding_dim=128, walk_length=80,
                 context_size=20, walks_per_node=10, sparse=True).to(device)

        loader = model.loader(batch_size=128, shuffle=True, num_workers=1)
        optimizer = torch.optim.SparseAdam(model.parameters(), lr=0.01)

        for epoch in range(EPOCHS):
            model.train()

            # total_loss = 0
            for pos_rw, neg_rw in loader:
                optimizer.zero_grad()
                loss = model.loss(pos_rw.to(device), neg_rw.to(device))
                loss.backward()
                optimizer.step()
                # total_loss += loss.item()
            # total_loss = total_loss / len(loader)

        model.eval()
        out = model().cpu().detach().numpy()
        embeddings = np.zeros((out.shape[0], out.shape[1]+1))
        embeddings[:,1:] = out
        for node,idx in node2id.items():
            embeddings[idx,0] = node
        # print("1")    
        result = ''
        for embedding in embeddings:
            result = str([int(embedding[0])] + embedding[1:].tolist()).replace('[','').replace(']','').replace(',',' ') +'\t'
            print(result)