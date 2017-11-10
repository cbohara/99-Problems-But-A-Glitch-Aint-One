#!/bin/bash


# install Datadog: https://app.datadoghq.com/account/settings#agent/aws
echo "Installing datadog agent..."
DD_API_KEY=98a8ee3247f2d6abbe1b65b81b5d5be6 bash -c "$(curl -L https://raw.githubusercontent.com/DataDog/dd-agent/master/packaging/datadog-agent/source/install_agent.sh)"
if [ $? != 0 ]; then
  echo "Error installing datadog agent. Exiting..."
fi

## configure necessary components for sending metrics to datadog
CONF_DIR=/etc/dd-agent/conf.d

echo "Changing permissions of conf dir..."
sudo chmod -R 777 ${CONF_DIR}

#### YARN
echo "Configuring YARN..."
sudo printf "init_config:\n\ninstances:\n  - resourcemanager_uri: http://$(hostname):8088\n" > ${CONF_DIR}/yarn.yaml


#### MR
echo "Configuring MapReduce..."

sudo printf "instances:\n  - resourcemanager_uri: http://$(hostname):8088\n    cluster_name: rtbingestion\n    collect_task_metrics: true\n    tags:\n      - application:rtbingestion\n      - role:charlie\ninit_config:\n  general_counters:\n   - counter_group_name: 'org.apache.hadoop.mapreduce.TaskCounter'\n     counters:\n      - counter_name: 'MAP_INPUT_RECORDS'\n      - counter_name: 'MAP_OUTPUT_RECORDS'\n" > ${CONF_DIR}/mapreduce.yaml 

sudo cat /home/hadoop/dd.cfg >> ${CONF_DIR}/mapreduce.yaml
sudo cat ${CONF_DIR}/mapreduce.yaml

echo "Config test"
sudo /etc/init.d/datadog-agent configtest 

## restart datadog agent
echo "Restarting datadog agent"
sudo /etc/init.d/datadog-agent restart 
exit 0
