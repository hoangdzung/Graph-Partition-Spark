import argparse
from collections import defaultdict
from tqdm import tqdm 
import random 
import subprocess
import os 
import numpy as np 
import metis 
import networkx as nx 

parser = argparse.ArgumentParser()
parser.add_argument('whole')
parser.add_argument('core_size', type=int)
parser.add_argument('npart',type=int)
parser.add_argument('--feats')
parser.add_argument('--outdir')
args = parser.parse_args()

G = nx.read_edgelist(args.whole)

print("Sampling core nodes...")
core_nodes = set([random.choice([node for node in G.nodes])])
while(True):
    cand_nodes = set()
    for node in core_nodes:
        cand_nodes = cand_nodes.union([neigh for neigh in G.neighbors(node) if neigh not in core_nodes]) 
    if len(cand_nodes) + len(cand_nodes) < args.core_size:
        core_nodes = core_nodes.union(cand_nodes)
    else:
        cand_nodes = random.sample(list(cand_nodes), k = args.core_size - len(core_nodes))
        core_nodes = core_nodes.union(cand_nodes)
        break
print("Constructing the rest of graph...")
not_core_G = G.subgraph([node for node in G if node not in core_nodes])
print("Partitioning the rest of graph...")
ncut, parts = metis.part_graph(not_core_G, args.npart)
part2nodes = defaultdict(list)
for node, p in zip(G.nodes, parts):
    part2nodes[p].append(node) 

if args.outdir is not None:
    if not os.path.isdir(args.outdir):
        os.makedirs(args.outdir)

    if args.feats is not None:
        feats = {}
        for node, feat in tqdm(enumerate(open(args.feats)), desc="Read feat"):
            feats[str(node)] = np.array(list(map(float,feat.split())))
        core_nodes =  list(core_nodes)
        for p, nodes in part2nodes.items():
            with open(os.path.join(args.outdir,'part_{}.txt.feat'.format(p)),'w') as f:
                for node in tqdm(nodes+core_nodes,desc="Write feat {}".format(p)):
                    f.write("{} {}\n".format(node, " ".join(map(str, feats[node]))))
    for p, nodes in part2nodes.items():
        nx.write_edgelist(G.subgraph(nodes+core_nodes), os.path.join(args.outdir,'part_{}.txt'.format(p)), data=False )

print("Ncut: ",ncut),
print("Avg core deg: ", np.mean(len(G.degree(core_node)) for core_node in core_nodes) )
print("Part size: ", [len(i) for i in parts] )
