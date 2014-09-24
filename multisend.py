#!/usr/bin/env python

# send data to graphite

MAX_TRY=10
AUTH_HEADER={}

import sys

raw=[];
for line in sys.stdin :
  raw.append(line.split())

if len(raw) == 0:
  exit(0);

raw.reverse() 

#[path,value,timestamp] 
#change to
#[(path, (timestamp, value)), ...]

name_ts_hash={}
unique_raw=[]
for metric in raw:
  if (metric[0], metric[2]) in name_ts_hash:
    continue
  name_ts_hash[ (metric[0], metric[2]) ] = True
  unique_tuple=( metric[0], (metric[2],metric[1]) )
  unique_raw.append(unique_tuple);
raw=unique_raw

import socket
import time
import datetime
import urllib
import urllib2
import re
from multiprocessing.dummy import Pool as ThreadPool
import pickle
import struct

# your graphite server ip
host='XXX.XXX.XXX.XXX'
#using The pickle protocol,the default port is 2004
port=2004 
timeout=20  
socket.setdefaulttimeout(timeout)#
def graphite_send(list_of_data):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.connect((host, port))
  payload = pickle.dumps(list_of_data, protocol=2)
  header = struct.pack("!L", len(payload))
  message = header + payload
  sock.send(message)
  sock.close()

def get_from_until(ts):
  if not hasattr(get_from_until, 'cache'):
    get_from_until.cache = {}
  ts=str(ts)
  if not ts in get_from_until.cache :
    dt = datetime.datetime.fromtimestamp(int(ts))
    get_from_until.cache[ts] = 'from=%s&until=%s' % (dt.strftime('%Y%m%d'), (dt + datetime.timedelta(days=1)).strftime('%Y%m%d'))
  return get_from_until.cache[ts];

url_tpl='http://' + host + '/render/?format=raw&%s&target=%s'

metric_value_pattern=re.compile('\|[^0-9]*([0-9]+(?:\.[0-9]+)?)')

#(path, (timestamp, value)), ...]
def has_metric(d):
  name=d[0]
  value=d[1][1]
  ts=d[1][0]
  if not hasattr(has_metric, 'cache'):
    has_metric.cache = {}
  name_ts = (name, ts)
  if not name_ts in has_metric.cache :
    url = url_tpl % ( get_from_until(ts), urllib.quote(name) )
    try:
      req = urllib2.Request(url, None, AUTH_HEADER)
      response = urllib2.urlopen(req).read()
      has_metric.cache[ name_ts ] = round( float( metric_value_pattern.search( response ).group(1) ), 2 )
    except:
      pass
    if has_metric.cache.get( name_ts ) != round( float(value), 2 ):
      return d


start=time.time()

data_pending_sent=raw

sys.stderr.write('finish sending to graphite ,begin to check the data  \n');

pool_size=100
size=100 # send 100 lines every times
count=len(data_pending_sent)/size+1
miss_data=[]

for i in range(count):
  if i == count-1:
    data = data_pending_sent[size*i:]
  else:
    data = data_pending_sent[size*i:size*(i+1)]

  try_count=0

  while True:
    #send data to graphite
    graphite_send(data)
    time.sleep(5);

    data_to_be_sent = [];

    #multithread to query url to check the data is stored in graphite or not
    pool = ThreadPool(pool_size)
    results = pool.map(has_metric, data)
    pool.close() 
    pool.join()

    
    for result in results:
      if result is not None:
        data_to_be_sent.append(result)

    data = data_to_be_sent

    try_count += 1;
    if len(data) <= 0 :
      break;
    if try_count >= MAX_TRY:
      time.sleep(10)
      break;
    sys.stderr.write( 'retry No. %.2d time / %s retries \n' % (try_count,len(data)) );
  
  left_to_send=len(data_pending_sent) - i*size
  sys.stderr.write( 'left %.2d lines to send \n' % left_to_send );

  miss_data.extend(data)

for d in miss_data:
  sys.stdout.write(d[0] + ' ' + d[1][1] + ' ' + d[1][0] + '\n' );

sys.stderr.write( 'end at %s: %s seconds spent,  %s saved / %s dropped \n' % (time.strftime('%F %H:%M:%S %z'), int((time.time() - start) * 1000) / 1000.0, len(raw) - len(miss_data), len(miss_data) ) );








