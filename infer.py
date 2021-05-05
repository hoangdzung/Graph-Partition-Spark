import networkx as nx 
import sys 
from collections import defaultdict

G = nx.read_edgelist(sys.argv[1])
part2nodes = defaultdict(list) 
for line in open(sys.argv[2]):
    data = line.strip().split()
    part2nodes[data[1]].append(data[0])

for nodes in part2nodes.values():
    subG = G.subgraph(nodes)
    print(len([len(c) for c in sorted(nx.connected_components(subG), key=len, reverse=True)]))


