import sys
import networkx as nx 
G = nx.read_edgelist(sys.argv[1])
n_nodes = int(sys.argv[2])
print(sorted([G.degree(node) for node in G], reverse=True)[:n_nodes])
print(n_nodes/len(G))
