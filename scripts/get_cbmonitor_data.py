#!/usr/bin/env python2.7

'''
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.

The script downloads the raw cbmonitor data for a given list of runs and dumps
some performance metrics to file (JSON and CSV formats).

Usage: get_cbmonitor_data.py --job-list \
       <project1>:<number1>[:'<label1>'] [<project2>:<number2> ..] \
       --output-dir <output_dir>
(e.g., get_cbmonitor_data.py --job-list \
       hera-pl:60:'RocksDB low OPS' hera-pl:67 --output_dir . )
'''

import argparse
import collections
import csv
import gzip
import json
import numpy
import re
from StringIO import StringIO
import sys
import urllib2


# data format: [[timestamp1,value1],[timestamp2,value2],..]
def downloadData(url):
    print("downloading: " + url)
    try:
        # Note: urllib2 module sends HTTP/1.1 requests with Connection:close
        # header included
        request = urllib2.Request(url)
        request.add_header('Accept-encoding', 'gzip')
        response = urllib2.urlopen(request, timeout=10)
    except urllib2.URLError, e:
        raise Exception("'urlopen' error, url not correct or user not "
                        "connected to VPN: %r" % e)

    if response.info().get('Content-Encoding') == 'gzip':
        buf = StringIO(response.read())
        f = gzip.GzipFile(fileobj=buf)
        return f.read()
    else:
        return response.read()


def getAverage(url):
    accumulator = 0.0
    pairs = json.loads(downloadData(url))
    for pair in pairs:
        accumulator += float(pair[1])
    return accumulator/len(pairs)


def getAverageFromList(urls, per_single_node):
    accumulator = 0.0
    for url in urls:
        accumulator += getAverage(url)
    return (accumulator/len(urls) if per_single_node else accumulator)


def getMax(url):
    maximum = 0.0
    pairs = json.loads(downloadData(url))
    for pair in pairs:
        value = float(pair[1])
        if value > maximum:
            maximum = value
    return maximum


def getP99(url):
    valueList = []
    pairs = json.loads(downloadData(url))
    for pair in pairs:
        valueList.append(float(pair[1]))
    return numpy.percentile(valueList, 99.0)


usage = ("Usage: get_cbmonitor_data.py --job-list "
         "<project1>:<number1>[:'<label1>'] [<project2>:<number2> ..] "
         "--output-dir <output_dir> "
         "\n\t(e.g., get_cbmonitor_data.py --job-list "
         "hera-pl:60:'RocksDB low OPS' hera-pl:67 hera-pl:83 --output-dir . )")

ap = argparse.ArgumentParser()

try:
    ap.add_argument('--job-list', nargs='+')
    ap.add_argument('--output-dir')
except:
    print(usage)
    sys.exit()

args, leftovers = ap.parse_known_args()

if (args.job_list is None) or (args.output_dir is None):
    print(usage)
    sys.exit()

job_list = args.job_list
output_dir = args.output_dir

print("Job list: " + str(job_list))
print("Output dir: " + output_dir)


byteToMBConversionFactor = 1.0/(1024*1024)
host = "http://cbmonitor.sc.couchbase.com:8080"

# Keep data for all jobs to build an aggregated CSV file
data_list = []
# Main loop
for job in job_list:
    print("**************************************")
    print("Job: " + job)
    print("**************************************")

    array = job.split(":")
    if len(array) < 2:
        print(usage)
        sys.exit()
    project = array[0]
    number = array[1]
    label = array[2] if (len(array) == 3) else ""

    consoleText = downloadData("http://perf.jenkins.couchbase.com/job/" +
                               project + "/" + number + "/consoleText")

    if consoleText.find("snapshot=") == -1:
        print("Snapshot not found for job" + job + ", probably the job has "
              "been aborted. Skipping..")
        continue
    snapshot = re.search("snapshot=.*", consoleText).group(0).split("=")[1]
    print("snapshot: " + snapshot)

    '''
    Regex to get nodes IP from e.g.:
        2018-01-16T11:35:11 [INFO] Getting memcached port from 172.23.96.100
        2018-01-16T11:35:11 [INFO] Getting memcached port from 172.23.96.101
        2018-01-16T11:35:11 [INFO] Getting memcached port from 172.23.96.102
        2018-01-16T11:35:11 [INFO] Getting memcached port from 172.23.96.103
    '''
    memcachedMatchAll = re.findall("Getting memcached port from.*",
                                   consoleText)
    nodes = []
    for match in memcachedMatchAll:
        nodes.append(match.split("from")[1].strip().replace(".", ""))
    nodes = set(nodes)
    print("nodes: " + str(nodes))

    '''
    Regex to get bucket name from e.g.:
        2018-01-16T09:19:17 [INFO] Adding new bucket: bucket-1
    '''
    bucket = re.search("Adding new bucket.*", consoleText) \
        .group(0) \
        .split(":")[1] \
        .strip()
    print("bucket: " + bucket)

    # url format: [host + "/" + "<dataset>" + snapshot [+ "<ip/bucket>"] +
    #              "/" + "<resource>"]
    base_url_ns_server = host + "/" + "ns_server" + snapshot + bucket
    base_url_spring_latency = host + "/" + "spring_latency" + snapshot + bucket

    # ops
    ops = host + "/" + "ns_server" + snapshot + "/ops"
    # latency
    latency_get = base_url_spring_latency + "/latency_get"
    latency_set = base_url_spring_latency + "/latency_set"
    # other interesting timings
    avg_bg_wait_time = base_url_ns_server + "/avg_bg_wait_time"
    avg_disk_commit_time = base_url_ns_server + "/avg_disk_commit_time"
    # read/write amplification
    data_rps = []
    data_wps = []
    data_rbps = []
    data_wbps = []
    for node in nodes:
        base_url_iostat = host + "/" + "iostat" + snapshot + node
        data_rps.append(base_url_iostat + "/data_rps")
        data_wps.append(base_url_iostat + "/data_wps")
        data_rbps.append(base_url_iostat + "/data_rbps")
        data_wbps.append(base_url_iostat + "/data_wbps")
    # disk amplification
    couch_total_disk_size = base_url_ns_server + "/couch_total_disk_size"
    # memory usage
    base_url_atop = host + "/" + "atop" + snapshot
    memcached_rss = []
    for node in nodes:
        memcached_rss.append(base_url_atop + node + "/memcached_rss")
    mem_used = base_url_ns_server + "/mem_used"
    # cpu usage
    memcached_cpu = []
    for node in nodes:
        memcached_cpu.append(base_url_atop + node + "/memcached_cpu")

    # Perfrunner collects the 'spring_latency' dataset for 'latency_set' and
    # 'latency_get' only on some test configs (i.e., synchronous clients)
    hasLatencySet = False
    hasLatencyGet = False
    hasLatency = True if (consoleText.find("spring_latency") != -1) else False
    if hasLatency:
        springLatency = downloadData(base_url_spring_latency)
        hasLatencySet = (True if (springLatency.find("latency_set") != -1)
                         else False)
        hasLatencyGet = (True if (springLatency.find("latency_get") != -1)
                         else False)

    data = collections.OrderedDict([
        ('job', project + ":" + number),
        ('label', label),
        ('snapshot', snapshot),
        ('ops', '{:.2f}'.format(getAverage(ops))),
        ('latency_set (ms)', '{:.2f}'.format(getAverage(latency_set))
                            if hasLatencySet else 'N/A'),
        ('latency_get (ms)', '{:.2f}'.format(getAverage(latency_get))
                            if hasLatencyGet else 'N/A'),
        ('latency_set P99 (ms)', '{:.2f}'.format(getP99(latency_set))
                                if hasLatencySet else 'N/A'),
        ('latency_get P99 (ms)', '{:.2f}'.format(getP99(latency_get))
                                if hasLatencyGet else 'N/A'),
        ('avg_disk_commit_time (ms)', '{:.2f}'
                                     .format(getAverage(avg_disk_commit_time) *
                                             1000)),
        ('avg_bg_wait_time (ms)', '{:.2f}'.format(getAverage(avg_bg_wait_time) /
                                                 1000)),
        ('avg_disk_commit_time P99 (ms)', '{:.2f}'
                                     .format(getP99(avg_disk_commit_time) *
                                             1000)),
        ('avg_bg_wait_time P99 (ms)', '{:.2f}'.format(getP99(avg_bg_wait_time) /
                                                     1000)),
        ('data_rps (iops)', '{:.2f}'.format(getAverageFromList(data_rps, True))),
        ('data_wps (iops)', '{:.2f}'.format(getAverageFromList(data_wps, True))),
        ('data_rbps (MB/s)', '{:.2f}'.format(
                                        getAverageFromList(data_rbps, True) *
                                        byteToMBConversionFactor
                                     )),
        ('data_wbps (MB/s)', '{:.2f}'.format(
                                        getAverageFromList(data_wbps, True) *
                                        byteToMBConversionFactor
                                    )),
        ('couch_total_disk_size (MB)', int(getAverage(couch_total_disk_size) *
                                          byteToMBConversionFactor)),
        ('max couch_total_disk_size (MB)', int(getMax(couch_total_disk_size) *
                                              byteToMBConversionFactor)),
        ('mem_used (MB)', '{:.2f}'.format(
                                     getAverage(mem_used) *
                                     byteToMBConversionFactor
                                 )),
        ('memcached_rss (MB, all nodes)', '{:.2f}'.format(
                                          getAverageFromList(memcached_rss,
                                                             False) *
                                          byteToMBConversionFactor
                                      )),
        ('memcached_cpu', '{:.2f}'.format(getAverageFromList(memcached_cpu,
                                                            True)))
    ])

    data_list.append(data)

    # Write to JSON
    json_file = output_dir + "/" + project + "-" + number + ".json"
    file = open(json_file, "wb")
    file.write(json.dumps(data))
    file.close()
    print("Data written to " + json_file)

# Write to CSV
csv_file = output_dir + "/" + "data.csv"
file = open(csv_file, "wb")
dictWriter = csv.DictWriter(file, data.keys())
dictWriter.writeheader()
for data in data_list:
    dictWriter.writerow(data)
file.close()
print("Aggregated data for all jobs written to " + csv_file)
