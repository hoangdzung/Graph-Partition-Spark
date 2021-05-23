#!/usr/bin/python3 

import torch
from torch_geometric.data import Data
from torch_geometric.nn import Node2Vec
import numpy as np
import networkx as nx 
import sys 
import smart_open
import subprocess
import re 
import os 
import boto3
import sys 

EPS = 1e-15

EPOCHS=5
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
# device = torch.device('cpu')
s3 = boto3.resource('s3')
bucket = s3.Bucket('graphframes-sh2')

def check_file(name):
    if name in os.listdir('/mnt/tmp'):
        return True, True
    else:
        ckpt_files = [i for i in bucket.objects.filter(Prefix='ckpt')]
        return name in ckpt_files, False

def copy_from_local(local_ckpt_path, shared_ckpt_path):
    s3.meta.client.upload_file(local_ckpt_path, 'graphframes-sh2', shared_ckpt_path)

def copy_to_local(shared_ckpt_path, local_ckpt_path):
    s3.meta.client.download_file(shared_ckpt_path, 'graphframes-sh2', local_ckpt_path)

for line in sys.stdin:
    path = line.strip()
    partid = re.search('(?<=/part_)[0-9]+(?=.txt*)',path).group(0)
    local_ckpt_path = os.path.join('/mnt/tmp', 'ckpt'+partid +'.pt')
    shared_ckpt_path = 'ckpt'+partid +'.pt'
    exist, is_local = check_file(shared_ckpt_path)

    if exist and not is_local:
        copy_to_local(shared_ckpt_path, local_ckpt_path)
        success = os.path.isfile(local_ckpt_path)
        subprocess.call(['touch', '/mnt/tmp/{}_copy_to_local_{}'.format(partid, success)])

    if exist and (is_local or success):
        node2id, model, curr_epoch = torch.load(local_ckpt_path) 
    else:
        curr_epoch = -1
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

        edge_index = torch.tensor(np.array(edge_list).T, dtype=torch.long)

        data = Data(edge_index=edge_index)

        model = Node2Vec(data.edge_index, embedding_dim=128, walk_length=4,
                context_size=2, walks_per_node=2, sparse=True).to(device)

    loader = model.loader(batch_size=2000, shuffle=True, num_workers=12)
    optimizer = torch.optim.SparseAdam(model.parameters(), lr=0.01)

    for epoch in range(curr_epoch+1, EPOCHS):
        model.train()

        # total_loss = 0
        for pos_rw, neg_rw in loader:
            optimizer.zero_grad()
            loss = model.loss(pos_rw.to(device), neg_rw.to(device))
            loss.backward()
            optimizer.step()
            # total_loss += loss.item()
        # total_loss = total_loss / len(loader)
        subprocess.call(['touch', '/mnt/tmp/{}_train_epoch_{}'.format(partid, epoch)])
        torch.save([node2id, model, epoch], local_ckpt_path)
        copy_from_local(local_ckpt_path, shared_ckpt_path)
        if not os.path.isfile('/mnt/tmp/{}'.format(partid)):
            subprocess.call(['touch', '/mnt/tmp/{}_interrupt'.format(partid)])
            sys.exit(1)
        # subprocess.call(['touch', '/mnt/tmp/{}_5_{}'.format(partid,success)])

    model.eval()
    out = model().cpu().detach().numpy()
    embeddings = np.zeros((out.shape[0], out.shape[1]+1))
    embeddings[:,1:] = out
    for node,idx in node2id.items():
        embeddings[idx,0] = node

    for embedding in embeddings:
        result = str([int(embedding[0])] + embedding[1:].tolist()).replace('[','').replace(']','').replace(',',' ') +'\t'
        print(result)
    os.remove('/mnt/tmp/{}_interrupt'.format(partid))