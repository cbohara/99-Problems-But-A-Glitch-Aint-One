#!/bin/bash

EMR_NAME=$1
S3_LOG_DIR=s3://verve-opsdata-aws/emr-logs
S3_DEST_DIR=s3://verve-home/leeblackwell/vervathon/jhistlogs
TMP_DIR=tmp_$(date +%s)

mkdir ${TMP_DIR}

aws emr --profile verve-ops-data list-clusters > ${TMP_DIR}/allclusters

# get ids
jq --arg arg1 "${EMR_NAME}" '.Clusters[] | select(.Name==$arg1)' ${TMP_DIR}/allclusters | jq '.Id' > ${TMP_DIR}/ids_we_want

for i in $(cat ${TMP_DIR}/ids_we_want); do
  # Clean up quotes
  id=$(echo $i | tr -d \")   
  echo "Cluster ID: $id"
  echo "  Downloading mapreduce jobhistory logs locally..."
  aws s3 --profile verve-ops-data cp --recursive ${S3_LOG_DIR}/${id} ${TMP_DIR}/jhist --exclude "*" --include "*.jhist.gz" 
  if [ $? != 0 ]; then
    echo "  Error downloading logs. Exiting..."
    exit
  fi
  filename=$(find ${TMP_DIR}/jhist -type f)
  echo "  Tailing jobhistory log and uploading to ${S3_DEST_DIR}..."
  tail -1 ${filename} > ${TMP_DIR}/${EMR_NAME}_${id}_jhist.json
  aws s3 --profile personal cp ${EMR_NAME}_${id}_jhist.json ${S3_DEST_DIR}
  echo "  Removing full jobhistory log..."
  rm -r ${TMP_DIR}/jhist  
done



#rm -r ${TMP_DIR}
