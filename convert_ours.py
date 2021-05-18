import argparse
from collections import defaultdict
from tqdm import tqdm 
import random 
import subprocess
import os 
import numpy as np 

parser = argparse.ArgumentParser()
parser.add_argument('whole')
parser.add_argument('core')
parser.add_argument('part')
parser.add_argument('npart',type=int)
parser.add_argument('--out')
parser.add_argument('--feats')
args = parser.parse_args()

adj_list = defaultdict(set)
n_edges = 0
node2id = {}
assert os.path.isdir(args.part)
for pfile in tqdm(os.listdir(args.part), desc='Read part graph'):
    if not  pfile.startswith('part'):
        continue
    for line in open(os.path.join(args.part,  pfile)):
        node1, node2 = line.strip().split()
        if node1 == node2:
            continue
        if node2 in adj_list[node1]:
            continue
        n_edges += 1
        adj_list[node1].add(node2)
        adj_list[node2].add(node1)
        try:
            idx = node2id[node1]
        except:
            node2id[node1] = len(node2id)
        try:
            idx = node2id[node2]
        except:
            node2id[node2] = len(node2id)

n_nodes = len(adj_list)
id2node = {idx: node for node,idx in node2id.items()}
outgraph = str(random.random())
with open(outgraph, 'w') as f:
    f.write("{} {} 000\n".format(n_nodes, n_edges))
    for i in tqdm(range(n_nodes), desc="Write metis"):
        data = ""
        for neigh in adj_list[id2node[i]]:
            data += " " + str(node2id[neigh] + 1)
        data = data.strip() + '\n'
        f.write(data) 
subprocess.call(['gpmetis', outgraph, str(args.npart)])
node2part = {}

part2nodes = defaultdict(list)
for line in tqdm(open(args.core),desc="Read core graph"):
    node, part = line.strip().split()
    node2part[node] = 0
    part2nodes[0].append(node)

for idx,line in tqdm(enumerate(open(outgraph+'.part.'+str(args.npart))),desc="Read metis out"):
    part = int(line.strip())
    node2part[id2node[idx]] = part + 1
    part2nodes[part+1].append(id2node[idx])

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
        for i in range(1, args.npart+1):
            part2edges[i].append(line)
    elif part1 != 0 and part2 ==0:
        if part1 == -1:
            part1 = random.randint(1,args.npart)
            node2part[node1] = part1 
        part2edges[part1].append(line)  
    elif part2 != 0 and part1 ==0:
        if part2 == -1:
            part2 = random.randint(1,args.npart)
            node2part[node2] = part2 
        part2edges[part2].append(line)

    elif part2 == part1:
        if part1 == -1:
            part1 = random.randint(1,args.npart)
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
    if args.feats is not None:
        feats = {}
        for node, feat in tqdm(enumerate(open(args.feats)), desc="Read feat"):
            feats[str(node)] = np.array(list(map(float,feat.split())))
        corenodes =  part2nodes[0]
        for p, nodes in part2nodes.items():
            with open(args.out+'_{}.txt.feat'.format(p-1),'w') as f:
                for node in tqdm(nodes+corenodes,desc="Write feat {}".format(p)):
                    f.write("{} {}\n".format(node, " ".join(map(str, feats[node]))))
    for p, edges in part2edges.items():
        with open(args.out+'_{}.txt'.format(p-1),'w') as f:
            for edge in tqdm(edges,desc="Write part {}".format(p)):
                f.write(edge)
os.remove(outgraph)
os.remove(outgraph+'.part.'+str(args.npart))
print("Ncore: ", len(part2nodes[0]))
print("Ncut: ",n_cut),
print("Avg core deg: ", np.mean(list(core2deg.values())))
print("N_miss_node: ",n_miss_node)
print("Part size: ", [len(i) for i in part2nodes.values()] )
