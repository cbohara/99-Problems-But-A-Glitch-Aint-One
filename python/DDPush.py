#!/usr/local/bin/python2.7
import argparse
#from datadog import initialize, api
import datadog
import io
import json
import logging
import math
import os
import re
import socket
import time
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
def genCredObj(api_key, app_key):
  return '{"api_key":"%s", "app_key":"%s"}' % (api_key, app_key)
if __name__ == '__main__':
  clp = argparse.ArgumentParser()
#  clp.add_argument('--api-key')
#  clp.add_argument('--app-key')
#  clp.add_argument('--credfile', default='%s/.datadog' % (os.environ['HOME']))
  clp.add_argument('-v', '--verbose', action='store_true', default=False)
  clp.add_argument('--host', default=socket.gethostname())
  clp.add_argument('--metric-name', required=True)
  clp.add_argument('--metric-value', required=True, type=int)
  clp.add_argument('--metric-tag', default='')
  clp.add_argument('--metric-time', type=int, default=math.floor(time.time()))
  args = clp.parse_args()
  if args.verbose:
    logger.setLevel(logging.DEBUG)
  # assume the contents are a json string
  if re.match('^\d+(?:\.\d+){3}', args.host) is None:
    # only do this if host is NOT an ip address
    args.host = args.host.split('.')[0];
  
  logger.info('sending: %s %s %d %d' % (args.metric_name, args.host, args.metric_value, args.metric_time))
  creds = {
    'api_key':'d9cc4ca28e2f6f398b45b2b852bd15d8',
    'app_key':'d114396fbb5843a44ad606b089a99de0422ac454'  
  }
  datadog.initialize(**creds)
  datadog.api.Metric.send(metric=args.metric_name, points=(args.metric_time, args.metric_value), host=args.host, tags=(args.metric_tag));
