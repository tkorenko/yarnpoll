#!/usr/bin/env python

#   This script is expected to be called as Zabbix-agent's userparameter
# script for polling YARN REST API.
#
#   Typical invocation:
# ./yarnpoll.py <resourcepath> <itemkey>
# 
#   Examples:
#   Zabbix-agent's supposed usage:
# ./yarnpoll.py cluster/metrics yarn.clusterMetrics.appsCompleted
# ./yarnpoll.py cluster/metrics yarn.clusterMetrics.appsFailed
#
#   To check which itemkeys available, run:
# ./yarnpoll.py cluster/metrics -a

import json
import requests
import sys

# XXX: get rid of hardcoded address
baseurl     = 'http://localhost:8088'

debug_flag  = 0
dump_all    = 0
resourcepath = ''
itemkey     = ''
hdrs        = { 'Accept': 'application/json' }
pars        = { }

# --------------------------------------------------------------------------

def err_exit(msg):
    if (debug_flag == 0):
        print "ZBX_NOTSUPPORTED"
    else:
        print msg
    exit (0)

def printdbg(msg1, *msgs):
    if (debug_flag > 0):
        print msg1, '' if len(msgs) == 0 else msgs

def printDict(d):
    for key in d:
        print key, ":", d[key]

def flattenDictOfDicts(outDict, inObject, prefix):
    if isinstance(inObject, dict):
        for key in inObject:
            val = inObject[key]
            newPrefix = prefix + '.' + key
            flattenDictOfDicts(outDict, val, newPrefix)
    else:
        outDict.update({prefix: inObject})

# --------------------------------------------------------------------------

# Remove script name from ARGV
sys.argv.pop(0)

# Filter out diagnostic keys
argv_filtered = list()
for arg in sys.argv:
    if (arg == '-d'):
        debug_flag = 1
        continue
    elif (arg == '-a'):
        dump_all = 1
        debug_flag = 1
        continue
    argv_filtered.append(arg)

printdbg('argv_filtered  :', argv_filtered)

# Try to extract resourcepath from filtered ARGV
if (len(argv_filtered) == 0):
    err_exit('Error: missing <resourcepath> argument')

resourcepath = argv_filtered.pop(0)

#   We expect either <itemkey> in ARGV or 'dump_all' flag to be
# set, other combinations are invalid.
if (len(argv_filtered) == 0 and dump_all == 0):
    err_exit('Error: missing <itemkey> argument');

if (len(argv_filtered) > 0):
    itemkey = argv_filtered.pop(0)

printdbg("resourcepath   :", resourcepath)
printdbg("itemkey        :", itemkey)

# Compose an URL
url = baseurl + '/ws/v1/' + resourcepath

printdbg("URL            :", url)

# Query external resource with HTTP GET
try:
    reply = requests.get(url, headers = hdrs, params = pars)
    if (reply.status_code != requests.codes.ok):
        printdbg("Status Code    :", reply.status_code)
        err_exit('HTTP request failed')
except requests.exceptions.RequestException as e:
    printdbg('Caught EXCEPTION', e)
    err_exit('An exception occured while querying external resource')
except:
    err_exit('Other kind of exception, needs debugging')

printdbg("HTTP answer len:", len(reply.text))

# Verify reply length
if (len(reply.text) == 0):
    err_exit('Nothing to parse: got zero length response')

# Treat response as JSON
try:
    jsonResponse = reply.json();
except ValueError as e:
    printdbg('Caught EXCEPTION', e)
    err_exit('ValueError Exception')
except:
    err_exit('Other kind of exception, needs debugging')

printdbg("JSON Response  :", jsonResponse)

flatDict = {}

# Turn Dictionary of Dictionaries (JSON) into 'single-dimension' dictionary
flattenDictOfDicts(flatDict, jsonResponse, 'yarn')

# 'Dump All' mode is useless for Zabbix agent, thus an execution is terminated
if (dump_all != 0):
    printdbg('---------- Available item keys -----------')
    printDict(flatDict)
    exit(0)
    ## NOTREACHED ##

# Provide final response
if (itemkey in flatDict):
    printdbg('-------- Response to Zabbix Agent --------')
    print flatDict[itemkey]
else:
    err_exit('WARNING: Item key not found')
