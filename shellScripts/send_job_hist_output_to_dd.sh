#!/bin/bash

# Downloads the json jobhist logs from S3_INPUT_DIR
# and sends all counters to datadog

### GLOBAL VARS HERE (To be parameterized)
S3_INPUT_DIR=s3://verve-home/leeblackwell/vervathon/jhistlogs
TMP_DIR=tmp_$(date +%s)
AWS_PROFILE=verve-ops-data
PYPATH=../python

mkdir ${TMP_DIR}

# Pull down the logs
aws s3 --profile ${AWS_PROFILE} cp --recursive ${S3_INPUT_DIR}/ ${TMP_DIR}

# Loop through and send counters to Datadog
for file in ${TMP_DIR}/*.json; do
  fn=$(echo $file | awk -F'/' '{print $NF}')
  emr_name=$(echo $fn | cut -d'_' -f 1)
  cluster_id=$(echo $fn | cut -d'_' -f 2) 
  jq '.event | to_entries[]' $file > ${TMP_DIR}/entries
  jobid=$(jq '.value.jobid' ${TMP_DIR}/entries)
  finishtime=$(jq '.value.finishTime' ${TMP_DIR}/entries)
  finishedmaps=$(jq '.value.finishedMaps' ${TMP_DIR}/entries)
  finishedreduces=$(jq '.value.finishedReduces' ${TMP_DIR}/entries)
  failedmaps=$(jq '.value.failedMaps' ${TMP_DIR}/entries)
  failedreduces=$(jq '.value.failedReduces' ${TMP_DIR}/entries)
  jq '.value.totalCounters.groups[].counts[]' ${TMP_DIR}/entries > ${TMP_DIR}/allcounters
  jq -r "[.name , .value] | @csv" ${TMP_DIR}/allcounters > ${TMP_DIR}/${emr_name}_${cluster_id}_counters.csv
  for c in $(cat ${TMP_DIR}/${emr_name}_${cluster_id}_counters.csv); do
    key=$(echo $c | cut -d',' -f 1)
    skey=$(echo $key | tr -d \")
    val=$(echo $c | cut -d',' -f 2)
    echo "$skey ... $val"
    ${PYPATH}/DDPush.py --metric-name di.vervathon.test.$skey --metric-value $val
  done
  # this is to simulate time series a bit
  sleep 2
done

# Clean up this mess
#rm -r ${TMP_DIR}
