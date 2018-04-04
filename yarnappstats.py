#!/usr/bin/env python

import ConfigParser
import json
import requests
import sys
import time

__author__ = 'Taras Korenko'

ex_unexpected_struct = 'Unexpected input structure'
ex_arg_not_a_dict = 'Function argument is not a dictionary'
ex_arg_not_a_list = 'Function argument is not a list'
ex_queue_not_found = 'Queue not found'

def zbx_unsupp_exit():
    print 'ZBX_UNSUPPORTED'
    exit(0)

## --- AppsStats -------------------------------------------------------

AppsStatsDescrs = (
    ('finished.succeeded', 2, 'FINISHED', 'SUCCEEDED'),
    ('finished.failed'   , 2, 'FINISHED', 'FAILED'   ),
    ('finished.killed'   , 2, 'FINISHED', 'KILLED'   ),
    ('finished.undefined', 2, 'FINISHED', 'UNDEFINED'),
    ('failed'            , 1, 'FAILED'  , ''         ),
    ('killed'            , 1, 'KILLED'  , ''         ),
    ('other'             , 0, ''        , ''         )
)

def AppsStats_initObject():
    obj = { }

    for descr in AppsStatsDescrs:
        obj.update( { descr[0]: 0 } )

    return obj

def AppsStats_lookupCounterName(appState, appFinalStatus):
    counterName = AppsStatsDescrs[len(AppsStatsDescrs) - 1][0];

    for descr in AppsStatsDescrs:
        match = 0

        if (descr[1] > 0 and appState       == descr[2]):
            match += 1
        if (descr[1] > 1 and appFinalStatus == descr[3]):
            match += 1
        if (descr[1] == match):
            counterName = descr[0]
            break

    return counterName

def AppsStats_increaseCounter(appsStatsObj, appState, appFinalStatus):
    if not isinstance(appsStatsObj, dict):
        raise TypeError(ex_arg_not_a_dict)

    counterName = AppsStats_lookupCounterName(appState, appFinalStatus)
    appsStatsObj[ counterName ] += 1

def AppsStats_getCounter(appsStatsObj, counterName):
    if not isinstance(appsStatsObj, dict):
        raise TypeError(ex_arg_not_a_dict)

    if not counterName in appsStatsObj:
        raise KeyError('Key not found: ' + counterName)

    return appsStatsObj[counterName]

## --- QueuesStats -----------------------------------------------------

def QueuesStats_initObject():
    obj = { }
    return obj

def QueuesStats_getStats(qsObj, queueName, counterName):
    if not isinstance(qsObj, dict):
        raise KeyError(ex_arg_not_a_dict)

    if not queueName in qsObj:
        raise KeyError(ex_queue_not_found + ': ' + queueName)

    appsStatsObj = qsObj[queueName]

    return AppsStats_getCounter(appsStatsObj, counterName)

def QueuesStats_updateStats(qsObj, queueName, appState, appFinalStatus):
    if not isinstance(qsObj, dict):
        raise KeyError(ex_arg_not_a_dict)

    if not queueName in qsObj:
        appsStatsObj = AppsStats_initObject()
        qsObj.update( { queueName: appsStatsObj } )

    appsStatsObj = qsObj[queueName]

    AppsStats_increaseCounter(appsStatsObj, appState, appFinalStatus)

## --- AppsHistory -----------------------------------------------------

ah_appDescrkeys = ['id', 'state', 'finalStatus', 'queue', 'applicationType',
    'finishedTime']

def AppsHistory_initObject():
    obj = { }
    return obj

def AppsHistory_insertAppRecord(ahObj, appDescr):
    if not isinstance(ahObj, dict):
        raise KeyError(ex_arg_not_a_dict)

    if not isinstance(appDescr, dict):
        raise KeyError(ex_arg_not_a_dict)

    availableKeys = set(appDescr.keys())
    expectedKeys  = set(ah_appDescrkeys)

    if not expectedKeys.issubset( availableKeys ):
        print 'Missing AppDescr Keys: ', expectedKeys.difference(
            availableKeys )
        raise ValueError('Invalid input: some appDescr keys are missing')

    appId = appDescr['id']

    if appId in ahObj:
        # do not process duplicates
        return

    localAppDescr = {
        'state'           : appDescr['state'],
        'finalStatus'     : appDescr['finalStatus'],
        'queue'           : appDescr['queue'].lower(),
        'applicationType' : appDescr['applicationType'],
        'finishedTime'    : appDescr['finishedTime'],
        '_processed'      : '0'
    }

    ahObj.update( { appId : localAppDescr } )

# XXX consider better name for 'absTime'
def AppsHistory_removeOldRecords(ahObj, absTime):
    if not isinstance(ahObj, dict):
        raise KeyError(ex_arg_not_a_dict)

    if len(ahObj) == 0:
        return

    appsToRemove = []

    for appId in ahObj:
        # 'finishedTime' is expressed in miliseconds
        appFinTime = int(ahObj[appId]['finishedTime']) / 1000
        if appFinTime < int(absTime):
            appsToRemove.append(appId)

    for appId in appsToRemove:
        del ahObj[ appId ]

    return len(appsToRemove)

def AppsHistory_updateQueuesStats(ahObj, qsObj):
    if not isinstance(ahObj, dict):
        raise KeyError(ex_arg_not_a_dict)
    if not isinstance(qsObj, dict):
        raise KeyError(ex_arg_not_a_dict)

    processedAppsQty = 0

    for appId in ahObj:
        app = ahObj[appId]
        if 0 == int(app['_processed']):
            queueName      = app['queue']
            appState       = app['state']
            appFinalStatus = app['finalStatus']
            QueuesStats_updateStats(qsObj,
                queueName, appState, appFinalStatus)
            app['_processed'] = '1'
            processedAppsQty += 1

    return processedAppsQty

## --- LocalVars -------------------------------------------------------

def LocalVars_initObject():
    obj = { }
    return obj

def LocalVars_get(lvObj, key):
    if not isinstance(lvObj, dict):
        raise KeyError(ex_arg_not_a_dict)

    val = None

    if key in lvObj:
        val = lvObj[key]

    return val

def LocalVars_set(lvObj, key, val):
    if not isinstance(lvObj, dict):
        raise KeyError(ex_arg_not_a_dict)

    lvObj[key] = val

def LocalVars_addInt(lvObj, key, add):
    if not isinstance(lvObj, dict):
        raise KeyError(ex_arg_not_a_dict)

    prevVal = LocalVars_get(lvObj, key)
    if prevVal == None:
        prevVal = '0'

    lvObj[key] = str(int(prevVal) + int(add))

## --- ScriptState -----------------------------------------------------

scriptState = {
    'localVars'   : { },
    'appsHistory' : { },
    'queuesStats' : { }
}

def ScriptState_loadFromFile(ss, fname):
    with open(fname, 'r') as fh:
        try:
            jScriptState = json.load(fh)
        except ValueError as e:
            print 'Exception: ', fname, ':', e
            return

    if not isinstance(jScriptState, dict):
        print 'Invalid data in', fname
        return

    jLocalVars = jScriptState.pop('localVars')
    if isinstance(jLocalVars, dict):
        scriptState['localVars'] = jLocalVars;

    jAppsHistory = jScriptState.pop('appsHistory')
    if isinstance(jAppsHistory, dict):
        scriptState['appsHistory'] = jAppsHistory

    jQueuesStats = jScriptState.pop('queuesStats')
    if isinstance(jQueuesStats, dict):
        scriptState['queuesStats'] = jQueuesStats

def ScriptState_saveToFile(ss, fname):
    with open(fname, 'w') as fh:
        json.dump(ss, fh)

def ScriptState_jumpTo(ss, treePath):
    if treePath == None:
        return ss
    if not isinstance(treePath, list):
        raise ValueError(ex_arg_not_a_list)

    ptr = ss

    while len(treePath) > 0:
        if not isinstance(ptr, dict):
            print 'Cannot process ', treePath[0]
            return ptr
        if not treePath[0] in ptr:
            raise KeyError('Invalid node ' + treePath[0])

        ptr = ptr.pop(treePath.pop(0))

    return ptr

def ScriptState_jumpTo_safe(ss, treePath):
    try:
        leaf = ScriptState_jumpTo(ss, treePath)
    except:
        zbx_unsupp_exit()
        ## NOTREACHED ##

    if not isinstance(leaf, (int, long, basestring)):
        zbx_unsupp_exit()
        ## NOTREACHED ##

    return leaf

## --- YarnRMAppsPoller ------------------------------------------------

def _YarnRMAppsPoller_pollClusterAppsAPI(baseURL, finishedTime):
    finishedTimeMs = str(int(finishedTime) * 1000)
    hdrs = { 'Accept'       : 'application/json' }
    pars = { 'states'       : 'failed,killed,finished',
             'deSelects'    : 'resourceRequests',
             'finishedTimeBegin' : finishedTimeMs
    }
    url  = baseURL + '/ws/v1/cluster/apps'

    # Query external resource with HTTP GET
    try:
        reply = requests.get(url, headers = hdrs, params = pars)
        if (reply.status_code != requests.codes.ok):
            print 'Status Code:', reply.status_code
            print 'HTTP request failed'
            exit(0)
    except requests.exceptions.RequestException as e:
        print 'RequestException: ', e
        print 'An exception occured while querying external resource'
        exit(0)
    
    # Verify reply length
    if (len(reply.text) == 0):
        print 'HTTP answer len:', len(reply.text)
        print 'Nothing to process: got zero length response'
        exit(0)
    
    # Treat response as JSON
    try:
        jsonResponse = reply.json();
    except ValueError as e:
        print 'ValueError Exception: ', e
        exit(0)
    
    return jsonResponse

# (c) https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ \
#      ResourceManagerRest.html#Cluster_Applications_API
#---------------------------------------------------------------------------
#{
#  "apps": {
#    "app": [
#      {
#        "id": "application_1519830322804_0388", 
#        ...
#---------------------------------------------------------------------------
#   An exact structure is expected here ^, thus I step two levels in depth
# to reach a list of applications:
def _YarnRMAppsPoller_extractAppsList(jResp):
    if isinstance(jResp, dict):
        js2 = jResp.pop('apps')
        if isinstance(js2, dict):
            listOfApps = js2.pop('app')
            if not isinstance(listOfApps, list):
                raise ValueError(ex_unexpected_struct)
        else:
            raise ValueError(ex_unexpected_struct)
    else:
        raise TypeError(ex_arg_not_a_dict)

    return listOfApps

def YarnRMAppsPoller_getFinalizedAppsList(baseURL, finishedTime):
    jResp    = _YarnRMAppsPoller_pollClusterAppsAPI(baseURL, finishedTime)
    appsList = _YarnRMAppsPoller_extractAppsList(jResp)

    return appsList

## --- Config ----------------------------------------------------------

def Config_readFrom(fname, ioCfg):
    confParser = ConfigParser.RawConfigParser()
    confParser.read(fname)

    for key in ioCfg:
        val = confParser.get('global', key)
        ioCfg[key] = val

## --- fapps --------------------------------------------------------------
# Config file + CMD ARGV handling

# The following values should be supplied by script's config file:
cfg = {
    'baseurl'        : '',
    'state_filename' : '',
    'keep_history'   : ''
}

Config_readFrom('yarnappstats.cfg', cfg)

currentTime = int(time.time())

# Script's modes of operation
OP_POLL  = 'poll'
OP_PRINT = 'print'
OP_DUMP  = 'dump'
OP_MODES = (OP_POLL, OP_PRINT, OP_DUMP)

# Remove script name from ARGV
sys.argv.pop(0)

if 0 == len(sys.argv):
    print 'missing command'
    exit(0)

cmd = sys.argv.pop(0)
if not cmd in OP_MODES:
    print 'unknown command, choose from ', OP_MODES
    exit(0)

#---------------------------------------------------------------------------
# Load saved script state; missing state file should not break the execution

scriptState['appsHistory'] = AppsHistory_initObject()
scriptState['queuesStats'] = QueuesStats_initObject()
scriptState['localVars']   =   LocalVars_initObject()

try:
    ScriptState_loadFromFile(scriptState, cfg['state_filename'])
except IOError as e:
    if cmd == OP_PRINT:
        zbx_unsupp_exit()
        ## NOTREACHED ##
    else:
        print 'Exception: ', e

ahObj = scriptState['appsHistory']
qsObj = scriptState['queuesStats']
lvObj = scriptState['localVars']

#---------------------------------------------------------------------------
# OP_PRINT, OP_DUMP modes of operation are handled here

if (cmd != OP_POLL):
    # create and fill out fake variable 'lastpoll_ago':
    lastpoll_at = int(LocalVars_get(lvObj, 'lastpoll_at'))
    lastpoll_ago = currentTime - lastpoll_at
    LocalVars_set(lvObj, 'lastpoll_ago', str(lastpoll_ago))

if (cmd == OP_PRINT):
    # Zabbix-interface to script state
    ptr = ScriptState_jumpTo_safe(scriptState, sys.argv)
    print ptr
    exit(0)
    ## NOTREACHED ##

if (cmd == OP_DUMP):
    print '# Debug interface'
    ptr = ScriptState_jumpTo(scriptState, sys.argv)
    print json.dumps(ptr, indent = 4, sort_keys = True)
    exit(0)
    ## NOTREACHED ##

#---------------------------------------------------------------------------
# OP_POLL mode:

# XXX consider better name for 'olderThan_ts'
olderThan_ts = currentTime - int(cfg['keep_history'])

listOfApps = YarnRMAppsPoller_getFinalizedAppsList(cfg['baseurl'],
    olderThan_ts)

for app in listOfApps:
    AppsHistory_insertAppRecord(ahObj, app)

removedApps = AppsHistory_removeOldRecords(ahObj, olderThan_ts)

addedApps   = AppsHistory_updateQueuesStats(ahObj, qsObj)

# XXX removeme:
print 'appsHistory: added ', addedApps, ', removed ', removedApps

LocalVars_set(lvObj, 'lastpoll_at', str(currentTime))

ScriptState_saveToFile(scriptState, cfg['state_filename'])

