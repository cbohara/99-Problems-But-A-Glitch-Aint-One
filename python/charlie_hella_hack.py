#!/usr/bin/env python3
import sys
import os
import json
import boto3
import botocore

header = ['JOB_ID', 'FILE_BYTES_READ', 'FILE_BYTES_WRITTEN', 'FILE_READ_OPS', 'FILE_LARGE_READ_OPS', 'FILE_WRITE_OPS', 'HDFS_BYTES_READ', 'HDFS_BYTES_WRITTEN', 'HDFS_READ_OPS', 'HDFS_LARGE_READ_OPS', 'HDFS_WRITE_OPS', 'S3_BYTES_READ', 'S3_BYTES_WRITTEN', 'S3_READ_OPS', 'S3_LARGE_READ_OPS', 'S3_WRITE_OPS', 'TOTAL_LAUNCHED_MAPS', 'TOTAL_LAUNCHED_REDUCES', 'OTHER_LOCAL_MAPS', 'DATA_LOCAL_MAPS', 'SLOTS_MILLIS_MAPS', 'SLOTS_MILLIS_REDUCES', 'MILLIS_MAPS', 'MILLIS_REDUCES', 'VCORES_MILLIS_MAPS', 'VCORES_MILLIS_REDUCES', 'MB_MILLIS_MAPS', 'MB_MILLIS_REDUCES', 'MAP_INPUT_RECORDS', 'MAP_OUTPUT_RECORDS', 'MAP_OUTPUT_BYTES', 'MAP_OUTPUT_MATERIALIZED_BYTES', 'SPLIT_RAW_BYTES', 'COMBINE_INPUT_RECORDS', 'COMBINE_OUTPUT_RECORDS', 'REDUCE_INPUT_GROUPS', 'REDUCE_SHUFFLE_BYTES', 'REDUCE_INPUT_RECORDS', 'REDUCE_OUTPUT_RECORDS', 'SPILLED_RECORDS', 'SHUFFLED_MAPS', 'FAILED_SHUFFLE', 'MERGED_MAP_OUTPUTS', 'GC_TIME_MILLIS', 'CPU_MILLISECONDS', 'PHYSICAL_MEMORY_BYTES', 'VIRTUAL_MEMORY_BYTES', 'COMMITTED_HEAP_BYTES', 'BAD_ID', 'CONNECTION', 'IO_ERROR', 'WRONG_LENGTH', 'WRONG_MAP', 'WRONG_REDUCE', 'AMBIGUOUS_BID_REQUESTS', 'BID_REQUESTS', 'CENTROIDS', 'INVALID_REQUESTS', 'INVALID_REQUESTS_DIRTY_DEVICE', 'INVALID_REQUESTS_MISSING_CORE', 'INVALID_REQUESTS_WITH_BOGUS_DEVICE_ID', 'INVALID_REQUEST_CONTAINING_ESCAPED_TAB', 'MISSING_LAT_LON', 'NOBID_REQUESTS', 'REQUESTS_MATCHING_CENTROID', 'REQUESTS_NOT_MATCHING_CENTROID', 'TOTAL_REQUESTS', 'VALID_REQUESTS', 'REQUESTS_MATCHING_CENTROID', 'REQUESTS_NOT_MATCHING_CENTROID', 'TOTAL_REQUESTS', 'BYTES_READ', 'BYTES_WRITTEN']


def download_dir(client, resource, dist, local='/tmp', bucket='verve-home'):
	"""Download files from s3 into a local tmp directory"""
	paginator = client.get_paginator('list_objects')
	for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):
		if result.get('CommonPrefixes') is not None:
			for subdir in result.get('CommonPrefixes'):
				download_dir(client, resource, subdir.get('Prefix'), local, bucket)
		if result.get('Contents') is not None:
			for file in result.get('Contents'):
				local_path = local + os.sep + file.get('Key')
				if not os.path.exists(os.path.dirname(local_path)):
					os.makedirs(os.path.dirname(local_path))
				resource.meta.client.download_file(bucket, file.get('Key'), local + os.sep + file.get('Key'))


def read_in_logs(local_path):
	"""Read downloaded logs from s3 and append to list"""
	json_logs = []
	for json_file in os.listdir(local_path):
		tmp_file = os.path.join(local_path, json_file)
		if os.stat(tmp_file).st_size != 0:
			with open(tmp_file) as f:
				log = json.load(f, strict=False)
				json_logs.append(log)
	return json_logs


def parse_job_output(json_logs):
	"""Parse desired info from each json log and write values to list"""
	all_jobs = []
	for log in json_logs:
		try:
			jobid = log['event']['org.apache.hadoop.mapreduce.jobhistory.JobFinished']['jobid']
		except KeyError:
			continue
		else:
			job_values = [jobid]
			groups = log['event']['org.apache.hadoop.mapreduce.jobhistory.JobFinished']['totalCounters']['groups']
			for group in groups:
				counts = group['counts']
				for count in counts:
					for field in header:
						if field == count['name']:
							job_values.append(count['value'])
			all_jobs.append(job_values)
	return all_jobs


def get_header(json_logs):
	"""Get header for final report"""
	header = ['JOB_ID']
	json_log = json_logs[1]
	groups = json_log['event']['org.apache.hadoop.mapreduce.jobhistory.JobFinished']['totalCounters']['groups']
	for group in groups:
		counts = group['counts']
		for count in counts:
			header.append(count['name'])
	print(header)
	return header


def write_job_values(header, job_values):
	"""Write each row to report file"""
	with open('rtb_ingestion_log_report.csv', 'w') as f:
		header = ','.join(header)
		f.write(header+'\n')
		for job in job_values:
			print(len(job))
			job = [str(job) for job in job]
			job = ','.join(job)
			f.write(job+'\n')


def main():
	s3path = 'leeblackwell/vervathon/jhistlogs'
	client = boto3.client('s3')
	resource = boto3.resource('s3')
	local_path = download_dir(client, resource, s3path)
	local_path = '/tmp/'+s3path+'/'
	logs = read_in_logs(local_path)
	job_values = parse_job_output(logs)
	header = get_header(logs)
	report = write_job_values(header, job_values)


if __name__ == "__main__":
	sys.exit(main())
