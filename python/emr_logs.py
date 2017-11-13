#!/usr/bin/env python
import os
import sys
import json
import gzip
import boto3
import logging
import pandas as pd
from datetime import datetime
from collections import OrderedDict

from vrvm.myboto import s3
from vrvm.utils import vos
from vrvm.appwrap_new.appwrap import Appwrap


LOG = logging.getLogger(__name__)
LOG_FMT = '%(process)d - %(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s'


class EMRLogs(Appwrap):
    def add_config_options(self, p):
        g = p.add_argument_group('emr_logs')
        g.add('--emr_name', required=True, help='etlprod.prod.rtb-ingestion')
        g.add('--n', help='AWS limits GET response to 50 clusters - specify 50*n for total clusters to search')
        g.add('--finished_mr_job', help='ex: org.apache.hadoop.mapreduce.jobhistory.JobFinished')
        g.add('--failed_mr_job', help='ex: org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletion')
        g.add('--csv_name', help='name for output csv file')


    def list_all_clusters(self, emr_client, n=10):
        """Get EMR cluster info in the form of boto3 json responses"""
        # EMR client only returns 50 clusters per GET request
        # marker is a pagination token that indicates the next set of results to retrieve
        if self.args.n:
            n = int(self.args.n)
        marker = ''
        full_cluster_list = []
        for i in range(n):
            if i == 0:
                json_response = emr_client.list_clusters()
            else:
                json_response = emr_client.list_clusters(Marker=marker)

            for key, value in json_response.iteritems():
                if key == 'Marker':
                    marker = value
            full_cluster_list.append(json_response)
        return full_cluster_list


    def get_target_cluster_info(self, emr_client, full_cluster_list):
        """Get clusterID, date, and s3path for each cluster that matches emr_name"""
        target_clusters = []
        for cluster in full_cluster_list:
            for key, values in cluster.iteritems():
                if key == 'Clusters':
                    for value in values:
                        if value['Name'] == self.args.emr_name:
                            clusterID = value['Id']
                            date = value['Status']['Timeline']['CreationDateTime'].strftime('%Y/%m/%d')
                            s3path = 's3://verve-opsdata-aws/emr-logs/'+str(clusterID)+'/hadoop-mapreduce/history/'+date+'/000000/'

                            cluster_dict = {}
                            cluster_dict['clusterID'] = clusterID
                            cluster_dict['date'] = date
                            cluster_dict['s3path'] = s3path
                            target_clusters.append(cluster_dict)
        return target_clusters


    def get_filename_with_jobID(self, S3, target_clusters):
        """Get filename which contains jobID and store metadata to map jobID to clusterID"""
        cluster_metadata = []
        for cluster in target_clusters:
            files = S3.ls(cluster['s3path'])
            for filename in files:
                if filename.endswith('jhist.gz'):
                    jobID = filename[:17]
                    cluster['filename'] = str(filename)
                    cluster['jobID'] = str(jobID)
                    cluster_metadata.append(cluster)
        return cluster_metadata


    def copy_logs_to_tmp_dir(self, S3, cluster_metadata):
        """Copy log files from s3 to local tmp directory"""
        tmp_dir = '/tmp/'+self.args.emr_name+'/'
        vos.mkdir(tmp_dir)
        for cluster in cluster_metadata:
            S3.cp(cluster['s3path']+cluster['filename'], tmp_dir+cluster['filename'])
        return tmp_dir


    def get_final_log_json(self, tmp_dir):
        """Unzip and tail log for final job output json block"""
        final_outputs = []
        for filename in os.listdir(tmp_dir):
            filename = os.path.join(tmp_dir, filename)
            if os.stat(filename).st_size != 0:
                with gzip.open(filename, 'r') as f:
                    tail = f.read().splitlines()[-1]
                    final_outputs.append(tail)
        return final_outputs


    def split_logs(self, job_logs, mr_class):
        """Get job logs for specific mapreduce class"""
        split_jobs = []
        for job_log in job_logs:
            job_log = json.loads(job_log)
            try:
                log = job_log['event'][mr_class]
            except KeyError:
                continue
            else:
                split_jobs.append(log)
        return split_jobs


    def json_to_dict(self, cluster_metadata, job_outputs):
        """Create dictionary combining cluster metadata and job counters"""
        clusters = []
        for job in job_outputs:
            jobID = str(job['jobid'][:17])
            for cluster in cluster_metadata:
                if jobID == cluster['jobID']:
                    job_dict = OrderedDict()
                    job_dict['DATE'] = cluster['date']
                    job_dict['CLUSTER_ID'] = cluster['clusterID']
                    job_dict['JOB_ID'] = jobID

                    groups = job['totalCounters']['groups']
                    for group in groups:
                        counts = group['counts']
                        for count in counts:
                            job_dict[str(count['name'])] = count['value']
                    clusters.append(job_dict)
        return clusters


    def create_csv(self, clusters):
        """Create csv from final job logs"""
        if not self.args.csv_name:
            csv_name = self.args.emr_name+'.csv'
        else:
            csv_name = self.args.csv_name

        df = pd.DataFrame(clusters)
        df.to_csv(csv_name, index=False)


    def _run(self):
        S3 = s3.S3()
        emr_client = boto3.client('emr')
        full_cluster_list = self.list_all_clusters(emr_client)
        target_clusters = self.get_target_cluster_info(emr_client, full_cluster_list)
        cluster_metadata = self.get_filename_with_jobID(S3, target_clusters)
        tmp_dir = self.copy_logs_to_tmp_dir(S3, cluster_metadata)
        final_json_block = self.get_final_log_json(tmp_dir)
        finished_jobs = self.split_logs(final_json_block, self.args.finished_mr_job)
        clusters = self.json_to_dict(cluster_metadata, finished_jobs)
        self.create_csv(clusters)


if __name__ == "__main__":
    logging.basicConfig(filename='output.log', filemode='w', level=logging.DEBUG)
    EMRLogs().start()
