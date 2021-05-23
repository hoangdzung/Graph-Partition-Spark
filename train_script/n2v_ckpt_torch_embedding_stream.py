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

EPS = 1e-15

EPOCHS=5
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
# device = torch.device('cpu')
    
def check_file(name):
    if name in os.listdir('/home/hadoop'):
        return True, True
    else:
        args = "hdfs dfs -ls | awk '{print $8}'"
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        s_output, s_err = proc.communicate()
        all_dart_dirs = s_output.split()
        return name in all_dart_dirs, False

def copy_from_local(local, shared):
    put = subprocess.Popen(["sudo", "hdfs", "dfs", "-copyToLocal", local, shared], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=-1)
    s_output, s_err = put.communicate()
    return put.returncode == 0, s_output, s_err

def copy_to_local(shared, local):
    get = subprocess.Popen(["sudo", "hdfs", "dfs", "-copyFromLocal", shared, local], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=-1)
    s_output, s_err = get.communicate()
    return put.returncode == 0, s_output, s_err

for line in sys.stdin:
    path = line.strip()
    partid = re.search('(?<=/part_)[0-9]+(?=.txt*)',path).group(0)
    local_ckpt_path = os.path.join('/mnt/tmp', partid +'.pt')
    subprocess.call(['touch', '/mnt/tmp/{}_1'.format(partid)])
    shared_ckpt_path = partid +'.pt'
    exist, is_local = check_file(shared_ckpt_path)
    subprocess.call(['touch', '/mnt/tmp/{}_2'.format(partid)])

    if exist and not is_local:
        subprocess.call(['touch', '/mnt/tmp/{}_3_pre'.format(partid)])
        success, s_output, s_err = copy_to_local(shared_ckpt_path, local_ckpt_path)
        with open('/mnt/tmp/{}_3_{}'.format(partid, success), 'w') as f:
            f.write("out "+s_output+'\n')
            f.write("err "+s_err+'\n')
        # subprocess.call(['touch', '/mnt/tmp/{}_3_{}'.format(partid, success)])

    if exist and (is_local or success):
        subprocess.call(['touch', '/mnt/tmp/{}_4_pre'.format(partid)])
        node2id, model, curr_epoch = torch.load(local_ckpt_path) 
        subprocess.call(['touch', '/mnt/tmp/{}_4'.format(partid)])   
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
        subprocess.call(['touch', '/mnt/tmp/{}_4_pre'.format(partid)])
        torch.save([node2id, model, epoch], local_ckpt_path)
        subprocess.call(['touch', '/mnt/tmp/{}_4'.format(partid)])
        success, s_output, s_err = copy_from_local(local_ckpt_path, shared_ckpt_path)
        with open('/mnt/tmp/{}_5_{}'.format(partid, success), 'w') as f:
            f.write("out "+s_output+'\n')
            f.write("err "+s_err+'\n')
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