#!/bin/bash

# Start the deepfake detection process in this container.
# First, install and configure the AWS CLI for our use.
# Then, run the detector_interface python file to process
# the video files in S3.
echo '------------------------------------------'
echo 'Installing and configuring AWS CLI...'
./aws/install
aws --version
aws configure set aws_access_key_id "fake_key_id"
aws configure set aws_secret_access_key "fake_key"
aws configure set region "us-east-1"
aws configure set output "json"
echo 'AWS CLI installed and configured.'
python3 detector_interface.py