import argparse
from collections import defaultdict
from tqdm import tqdm 
import random 
import subprocess
import os 
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('whole')
parser.add_argument('part')
parser.add_argument('--outdir')
parser.add_argument('--feats')
args = parser.parse_args()

adj_list = defaultdict(set)
n_edges = 0
node2part = {}
part2nodes = defaultdict(list)
assert os.path.exists(args.part)

for node, part in tqdm(enumerate(open(args.part)),desc='Read part graph' ):
    part = int(part)
    node = str(node)
    node2part[node] = part 
    part2nodes[part].append(node)
        
npart = len(part2nodes)
part2edges = defaultdict(list)
for line in tqdm(open(args.whole), desc="Read all edges"):
    node1, node2 = line.strip().split()
    try:
        part1 = node2part[node1]
    except:
        part1 = -1
        node2part[node1] = -1 
    try:
        part2 = node2part[node2]
    except:
        part2 = -1
        node2part[node2] = -1

    part1, part2 = node2part[node1], node2part[node2]
    if part1 == 0 and part2 == 0:
        for i in range(1, npart+1):
            part2edges[i].append(line)
    elif part1 != 0 and part2 ==0:
        if part1 == -1:
            part1 = random.randint(1,npart)
            node2part[node1] = part1 
            part2nodes[part1].add(node1)
        part2edges[part1].append(line)  
    elif part2 != 0 and part1 ==0:
        if part2 == -1:
            part2 = random.randint(1,npart)
            node2part[node2] = part2 
            part2nodes[part2].add(node2)
        part2edges[part2].append(line)

    elif part2 == part1:
        if part1 == -1:
            part1 = random.randint(1,npart)
            node2part[node1] = part1 
            node2part[node2] = part1 
            part2nodes[part1].add(node1)
            part2nodes[part1].add(node2)
        part2edges[part1].append(line)
        
if args.out is not None:
    if not os.path.isdir(args.outdir):
        os.makedirs(args.outdir)

    if args.feats is not None:
        feats = {}
        for node, feat in tqdm(enumerate(open(args.feats)), desc="Read feat"):
            feats[str(node)] = np.array(list(map(float,feat.split())))
        for p, nodes in part2nodes.items():
            with open(os.path.join(args.outdir,'part_{}.txt.feat'.format(p)),'w') as f:
                for node in tqdm(nodes,desc="Write feat {}".format(p)):
                    f.write("{} {}\n".format(node, " ".join(map(str, feats[node]))))
    for p, edges in part2edges.items():
        with open(os.path.join(args.outdir,'part_{}.txt'.format(p)),'w') as f:
            for edge in tqdm(edges,desc="Write part {}".format(p)):
                f.write(edge)

