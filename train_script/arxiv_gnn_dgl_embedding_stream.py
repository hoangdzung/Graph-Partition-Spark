#!/usr/bin/python3 
import os 
os.environ["DGLBACKEND"] = 'pytorch'
os.environ["DGL_DOWNLOAD_DIR"] = '/home/hadoop/.dgl'

import dgl
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import DataLoader
import dgl.function as fn
import numpy as np
import dgl.nn.pytorch as dglnn
import sys 
import boto3 
import smart_open
import time
from torch_sparse import SparseTensor
from torch_geometric.utils.num_nodes import maybe_num_nodes
from torch_geometric.data import Data
from torch_geometric.nn import Node2Vec
try:
    import torch_cluster  # noqa
    random_walk = torch.ops.torch_cluster.random_walk
except ImportError:
    random_walk = None
# import getpass 

EPS = 1e-15

EPOCHS=5
FANOUTS=[10,25]
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
# device =  'cpu'
s3 = boto3.resource('s3')
torch.multiprocessing.set_sharing_strategy("file_system")

class RandomWalk():
    def __init__(self, edge_index, walk_length, context_size,
                 walks_per_node=1, p=1, q=1, num_negative_samples=1,
                 num_nodes=None, sparse=False ):
        
        if random_walk is None:
            raise ImportError('`Node2Vec` requires `torch-cluster`.')
        
        N = maybe_num_nodes(edge_index, num_nodes)
        row, col = edge_index
        self.adj = SparseTensor(row=row, col=col, sparse_sizes=(N, N))
        self.adj = self.adj.to('cpu')

        assert walk_length >= context_size

        self.walk_length = walk_length - 1
        self.context_size = context_size
        self.walks_per_node = walks_per_node
        self.p = p
        self.q = q
        self.num_negative_samples = num_negative_samples
        
    def loader(self, **kwargs):
        return DataLoader(range(self.adj.sparse_size(0)),
                          collate_fn=self.sample, **kwargs)


    def sample(self, batch):
        if not isinstance(batch, torch.Tensor):
            batch = torch.tensor(batch)
        batch = batch.repeat(self.walks_per_node)
        rowptr, col, _ = self.adj.csr()
        rw = random_walk(rowptr, col, batch, self.walk_length, self.p, self.q)
        if not isinstance(rw, torch.Tensor):
            rw = rw[0]
        walks = []
        num_walks_per_rw = 1 + self.walk_length + 1 - self.context_size
        for j in range(num_walks_per_rw):
            for i in range(1,self.context_size):
                walks.append(rw[:, [j,j+i]])    
        return torch.cat(walks, dim=0)


class NegativeSampler(object):
    def __init__(self, g, k, neg_share=False):
        self.weights = g.in_degrees().float() ** 0.75
        self.k = k
        self.neg_share = neg_share

    def __call__(self, g, eids):
        src, _ = g.find_edges(eids)
        n = len(src)
        if self.neg_share and n % self.k == 0:
            dst = self.weights.multinomial(n, replacement=True)
            dst = dst.view(-1, 1, self.k).expand(-1, self.k, -1).flatten()
        else:
            dst = self.weights.multinomial(n*self.k, replacement=True)
        src = src.repeat_interleave(self.k)
        return src, dst

class SAGE(nn.Module):
    def __init__(self,
                 in_feats,
                 n_hidden,
                 n_layers,
                 activation,
                 dropout):
        super().__init__()
        self.n_layers = n_layers
        self.n_hidden = n_hidden
        self.layers = nn.ModuleList()
        self.layers.append(dglnn.SAGEConv(in_feats, n_hidden, 'mean'))
        for i in range(1, n_layers):
            self.layers.append(dglnn.SAGEConv(n_hidden, n_hidden, 'mean'))
        self.dropout = nn.Dropout(dropout)
        self.activation = activation

    def forward(self, blocks, x):
        h = x
        for l, (layer, block) in enumerate(zip(self.layers, blocks)):
            h = layer(block, h)
            if l != len(self.layers) - 1:
                h = self.activation(h)
                h = self.dropout(h)
        return h

    def inference(self, g, x, device):
        """
        Inference with the GraphSAGE model on full neighbors (i.e. without neighbor sampling).
        g : the entire graph.
        x : the input of entire node set.
        The inference code is written in a fashion that it could handle any number of nodes and
        layers.
        """
        # During inference with sampling, multi-layer blocks are very inefficient because
        # lots of computations in the first few layers are repeated.
        # Therefore, we compute the representation of all nodes layer by layer.  The nodes
        # on each layer are of course splitted in batches.
        # TODO: can we standardize this?
        for l, layer in enumerate(self.layers):
            y = torch.zeros(g.num_nodes(), self.n_hidden)

            sampler = dgl.dataloading.MultiLayerFullNeighborSampler(1)
            dataloader = dgl.dataloading.NodeDataLoader(
                g,
                torch.arange(g.num_nodes()),
                sampler,
                batch_size=1000,
                shuffle=True,
                drop_last=False,
                num_workers=1)

            for input_nodes, output_nodes, blocks in dataloader:
                block = blocks[0].to(device)

                h = x[input_nodes].to(device)
                h = layer(block, h)
                if l != len(self.layers) - 1:
                    h = self.activation(h)
                    h = self.dropout(h)

                y[output_nodes] = h.detach().cpu()

            x = y
        return y

class CrossEntropyLoss(nn.Module):
    def forward(self, block_outputs, pos_graph, neg_graph):
        with pos_graph.local_scope():
            pos_graph.ndata['h'] = block_outputs
            pos_graph.apply_edges(fn.u_dot_v('h', 'h', 'score'))
            pos_score = pos_graph.edata['score']
        with neg_graph.local_scope():
            neg_graph.ndata['h'] = block_outputs
            neg_graph.apply_edges(fn.u_dot_v('h', 'h', 'score'))
            neg_score = neg_graph.edata['score']

        score = torch.cat([pos_score, neg_score])
        label = torch.cat([torch.ones_like(pos_score), torch.zeros_like(neg_score)]).long()
        loss = F.binary_cross_entropy_with_logits(score, label.float())
        return loss

feats = {}
for node, feat in enumerate(smart_open.open("s3://graphframes-sh2/data/ogbn-arxiv_text/features.txt")):
    feats[node] = np.array(list(map(float,feat.split())))

for line in sys.stdin:
    path = line.strip()
    node2id = dict()
    edge_list = set()
    X = []
    
    for line in smart_open.open(path):

        try:
            node1, node2  = list(map(int, line.strip().split()))

            try:
                id1 = node2id[node1]
            except:
                id1 = len(node2id)
                node2id[node1] = id1
                X.append(feats[node1])

            try:
                id2 = node2id[node2]
            except:
                id2 = len(node2id)
                node2id[node2] = id2
                X.append(feats[node2])
            
            edge_list.add((id1, id2))
        except:
            pass
   
    if len(node2id) == 0:
        print("")
    else:
        edge_list = np.array(list(edge_list))
        G = dgl.graph((edge_list[:,0], edge_list[:,1]), num_nodes=len(node2id))
        sampler = dgl.dataloading.MultiLayerNeighborSampler(FANOUTS)

        n_edges = G.num_edges()
        train_seeds = np.arange(n_edges)

        dataloader = dgl.dataloading.EdgeDataLoader(
            G, train_seeds, sampler, 
            negative_sampler=NegativeSampler(G, 1),
            batch_size=1000,
            shuffle=True,
            drop_last=False)
            # pin_memory=True)
            
        nfeat =  torch.tensor(X,dtype=torch.float)
        in_feats = nfeat.shape[1]
        model = SAGE(in_feats, 128, len(FANOUTS), F.relu, 0.5)
        model = model.to(device)
        loss_fcn = CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=0.01)

        for epoch in range(EPOCHS):

            # Loop over the dataloader to sample the computation dependency graph as a list of
            # blocks.

            for step, (input_nodes, pos_graph, neg_graph, blocks) in enumerate(dataloader):
                batch_inputs = nfeat[input_nodes].to(device)

                pos_graph = pos_graph.to(device)
                neg_graph = neg_graph.to(device)
                blocks = [block.int().to(device) for block in blocks]
                # Compute loss and prediction
                batch_pred = model(blocks, batch_inputs)
        #         print(batch_pred.shape, pos_graph, neg_graph)
                loss = loss_fcn(batch_pred, pos_graph, neg_graph)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

        model.eval()
        out = model.inference(G, nfeat, device).detach().numpy()
        embeddings = np.zeros((out.shape[0], out.shape[1]+1))
        embeddings[:,1:] = out
        for node,idx in node2id.items():
            embeddings[idx,0] = node
        result = ''
        for embedding in embeddings:
            result = str([int(embedding[0])] + embedding[1:].tolist()).replace('[','').replace(']','').replace(',',' ') +'\t'
            print(result)
