#!/bin/bash

# Given an EMR job name, this script will strip the mapreduce jobhistory log output and upload to s3 
# The output is a json string containing the counters and metrics of the finished MR job

EMR_NAME=$1
if [ -z "$1" ]; then
  echo "Must supply name of EMR job"
  exit
fi

### GLOBAL VARS HERE (To be parameterized)
S3_LOG_DIR=s3://verve-opsdata-aws/emr-logs
S3_DEST_DIR=s3://verve-home/leeblackwell/vervathon/jhistlogs
TMP_DIR=tmp_$(date +%s)
AWS_PROFILE=verve-ops-data

mkdir ${TMP_DIR}

aws emr --profile ${AWS_PROFILE} list-clusters > ${TMP_DIR}/allclusters

# get ids
jq --arg arg1 "${EMR_NAME}" '.Clusters[] | select(.Name==$arg1)' ${TMP_DIR}/allclusters | jq '.Id' > ${TMP_DIR}/ids_we_want

for i in $(cat ${TMP_DIR}/ids_we_want); do
  # Clean up quotes
  id=$(echo $i | tr -d \")   
  echo "Cluster ID: $id"
  echo "  Downloading mapreduce jobhistory logs locally..."
  aws s3 --profile ${AWS_PROFILE} cp --recursive ${S3_LOG_DIR}/${id} ${TMP_DIR}/jhist --exclude "*" --include "*.jhist.gz" 
  if [ $? != 0 ]; then
    echo "  Error downloading logs. Exiting..."
    exit
  fi
  filename=$(find ${TMP_DIR}/jhist -type f)
  echo "  Tailing jobhistory log and uploading to ${S3_DEST_DIR}/..."
  zcat < ${filename} | tail -1 > ${TMP_DIR}/${EMR_NAME}_${id}_jhist.json
  aws s3 --profile personal cp ${TMP_DIR}/${EMR_NAME}_${id}_jhist.json ${S3_DEST_DIR}/
  if [ $? != 0 ]; then
    echo "  Error copying tailed log to s3!" 
  fi
  echo "  Removing full jobhistory log..."
  rm -r ${TMP_DIR}/jhist  
done

# Success, let's clean up
rm -r ${TMP_DIR}
