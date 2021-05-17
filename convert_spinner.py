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
parser.add_argument('--out')
args = parser.parse_args()

adj_list = defaultdict(set)
n_edges = 0
node2part = {}
part2nodes = defaultdict(list)
assert os.path.isdir(args.part)
for pfile in tqdm(os.listdir(args.part), desc='Read part graph'):
    if not  pfile.startswith('part'):
        continue
    for line in open(os.path.join(args.part,  pfile)):
        node, part = line.strip().split()
        node, part = int(node), int(part)
        node2part[node] = part 
        part2nodes[part].append(node)

n_core = len(part2nodes[0])
npart = len(part2nodes) - 1
part2edges = defaultdict(list)
n_cut = 0
n_miss_node = 0
core2deg = {}
for line in tqdm(open(args.whole), desc="Read all edges"):
    node1, node2 = line.strip().split()
    try:
        part1 = node2part[node1]
    except:
        part1 = -1
        node2part[node1] = -1 
        n_miss_node += 1
    try:
        part2 = node2part[node2]
    except:
        part2 = -1
        node2part[node2] = -1
        n_miss_node += 1

    part1, part2 = node2part[node1], node2part[node2]
    if part1 ==0:
        core2deg[node1] = core2deg.get(node1, 0) + 1
    if part2 ==0:
        core2deg[node2] = core2deg.get(node2, 0) + 1
    if part1 == 0 and part2 == 0:
        for i in range(1, npart+1):
            part2edges[i].append(line)
    elif part1 != 0 and part2 ==0:
        if part1 == -1:
            part1 = random.randint(1,npart)
            node2part[node1] = part1 
        part2edges[part1].append(line)  
    elif part2 != 0 and part1 ==0:
        if part2 == -1:
            part2 = random.randint(1,npart)
            node2part[node2] = part2 
        part2edges[part2].append(line)

    elif part2 == part1:
        if part1 == -1:
            part1 = random.randint(1,npart)
            node2part[node1] = part1 
            node2part[node2] = part1 
        part2edges[part1].append(line)
        
    else:
        n_cut +=1
if args.out is not None:
    try:
        outdir = "/".join(args.out.split("/")[:-1])
        if not os.path.isdir(outdir):
            os.makedirs(outdir)
    except:
        pass 

    for p, edges in part2edges.items():
        with open(args.out+'_{}.txt'.format(p-1),'w') as f:
            for edge in tqdm(edges,desc="Write part {}".format(p)):
                f.write(edge)

print("Ncore: ", n_core)
print("Ncut: ",n_cut),
print("Avg core deg: ", np.mean(list(core2deg.values())))
print("N_miss_node: ",n_miss_node)
print("Part size: ", [i for i in part2nodes.values()] )
