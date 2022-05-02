#!/bin/bash -xe

# Non-standard and non-Amazon Machine Image Python modules:
# sudo pip3 install -U \
#   awscli            \
#   Boto3              \
#   s3fs==0.4.1          \
sudo python3 -m pip install awscli Boto3 s3fs==0.4.1