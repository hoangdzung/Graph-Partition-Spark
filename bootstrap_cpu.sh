#!/bin/bash
# sudo yum install -y python3-devel 
sudo pip3 install gensim==3.8.3
sudo pip3 install scikit-learn==0.22
sudo pip3 install  networkx 
sudo pip3 install torch==1.8.1+cpu torchvision==0.9.1+cpu torchaudio==0.8.1 -f https://download.pytorch.org/whl/torch_stable.html
sudo pip3 install  torch-scatter -f https://pytorch-geometric.com/whl/torch-1.8.0+cpu.html
sudo pip3 install  torch-sparse -f https://pytorch-geometric.com/whl/torch-1.8.0+cpu.html
sudo pip3 install  torch-cluster -f https://pytorch-geometric.com/whl/torch-1.8.0+cpu.html
sudo pip3 install  torch-spline-conv -f https://pytorch-geometric.com/whl/torch-1.8.0+cpu.html
sudo pip3 install dgl
sudo pip3 install  pytest-runner
sudo pip3 install  numba==0.50.1
sudo pip3 install boto3
sudo pip3 install smart_open
sudo /usr/bin/pip3 install --upgrade pip
sudo /usr/local/bin/pip3 install  torch-geometric

mkdir /home/hadoop/.dgl

aws s3 cp  s3://graphframes-sh2/train_script/n2v_torch_embedding_stream.py /home/hadoop
aws s3 cp  s3://graphframes-sh2/train_script/n2v_gensim_embedding_stream.py /home/hadoop
aws s3 cp  s3://graphframes-sh2/train_script/arxiv_gnn_rw_dgl_embedding_stream.py /home/hadoop
aws s3 cp  s3://graphframes-sh2/train_script/arxiv_gnn_dgl_embedding_stream.py /home/hadoop
chmod 777 /home/hadoop/n2v_torch_embedding_stream.py
chmod 777 /home/hadoop/n2v_gensim_embedding_stream.py
chmod 777 /home/hadoop/arxiv_gnn_rw_dgl_embedding_stream.py
chmod 777 /home/hadoop/arxiv_gnn_dgl_embedding_stream.py
