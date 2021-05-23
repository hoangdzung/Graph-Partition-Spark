#!/bin/bash
sudo yum install -y htop
aws s3 cp  s3://graphframes-sh2/train_script/save_hdfs.sh /home/hadoop
chmod 777 /home/hadoop/save_hdfs.sh
