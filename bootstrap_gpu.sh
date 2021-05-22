#!/bin/bash
sudo pip3 install gensim==3.8.3
sudo pip3 install scikit-learn==0.22
sudo pip3 install  networkx 
sudo pip3 install torch==1.6.0+cu92 torchvision==0.7.0+cu92 -f https://download.pytorch.org/whl/torch_stable.html
sudo pip3 install  torch-scatter==2.0.5+cu92 -f https://pytorch-geometric.com/whl/torch-1.6.0.html
sudo pip3 install  torch-sparse==0.6.7+cu92 -f https://pytorch-geometric.com/whl/torch-1.6.0.html
sudo pip3 install  torch-cluster==1.5.7+cu92 -f https://pytorch-geometric.com/whl/torch-1.6.0.html
sudo pip3 install  torch-spline-conv==1.2.0+cu92 -f https://pytorch-geometric.com/whl/torch-1.6.0.html
sudo pip3 install  pytest-runner
sudo pip3 install  numba==0.50.1
sudo pip3 install boto3
sudo pip3 install smart_open
sudo /usr/bin/pip3 install --upgrade pip
sudo /usr/local/bin/pip3 install  torch-geometric

mkdir /home/hadoop/.dgl

aws s3 cp  s3://graphframes-sh2/train_script/n2v_torch_embedding_stream.py /home/hadoop
aws s3 cp  s3://graphframes-sh2/train_script/n2v_ckpt_torch_embedding_stream.py /home/hadoop
aws s3 cp  s3://graphframes-sh2/train_script/n2v_gensim_embedding_stream.py /home/hadoop
aws s3 cp  s3://graphframes-sh2/train_script/arxiv_gnn_rw_dgl_embedding_stream.py /home/hadoop
aws s3 cp  s3://graphframes-sh2/train_script/arxiv_gnn_dgl_embedding_stream.py /home/hadoop
chmod 777 /home/hadoop/n2v_torch_embedding_stream.py
chmod 777 /home/hadoop/n2v_ckpt_torch_embedding_stream.py
chmod 777 /home/hadoop/n2v_gensim_embedding_stream.py
chmod 777 /home/hadoop/arxiv_gnn_rw_dgl_embedding_stream.py
chmod 777 /home/hadoop/arxiv_gnn_dgl_embedding_stream.py