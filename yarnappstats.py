#!/usr/bin/python2
"""A script for collecting Jobs statistics from YARN RM.

This script is intented to poll YARN Resource Manager and collect
statistical information about Applications/Jobs that reached their
final state.
"""
#.......................................................................

__author__ = 'Taras Korenko'

import ConfigParser
import json
import sys
import time

import requests


EX_UNEXPECTED_STRUCT = 'Unexpected input structure'
EX_ARG_NOT_A_DICT = 'Function argument is not a dictionary'
EX_ARG_NOT_A_LIST = 'Function argument is not a list'
EX_QUEUE_NOT_FOUND = 'Queue not found'


def zbx_unsupp_exit():
    """Hides script's guts from zabbix-agent"""
    print 'ZBX_UNSUPPORTED'
    exit(1)


_APPSSTATSDESCRS = (
    ('finished.succeeded', 2, 'FINISHED', 'SUCCEEDED'),
    ('finished.failed', 2, 'FINISHED', 'FAILED'),
    ('finished.killed', 2, 'FINISHED', 'KILLED'),
    ('finished.undefined', 2, 'FINISHED', 'UNDEFINED'),
    ('failed', 1, 'FAILED', ''),
    ('killed', 1, 'KILLED', ''),
    ('other', 0, '', '')
)


def appsstats_init_object():
    """Creates a structure for keeping statistics counters"""
    obj = {}

    for descr in _APPSSTATSDESCRS:
        obj[descr[0]] = 0

    return obj


def appsstats_lookup_counter_name(app_state, app_final_status):
    """Maps application's (state,finalStatus) to stats counter name"""
    counter_name = _APPSSTATSDESCRS[len(_APPSSTATSDESCRS) - 1][0]

    for descr in _APPSSTATSDESCRS:
        match = 0

        if descr[1] > 0 and app_state == descr[2]:
            match += 1
        if descr[1] > 1 and app_final_status == descr[3]:
            match += 1
        if descr[1] == match:
            counter_name = descr[0]
            break

    return counter_name


def appsstats_increase_counter(appsstats_obj, app_state, app_final_status):
    """Handles updates of an appropriate stats counter"""
    if not isinstance(appsstats_obj, dict):
        raise TypeError(EX_ARG_NOT_A_DICT)

    counter_name = appsstats_lookup_counter_name(app_state, app_final_status)
    appsstats_obj[counter_name] += 1


def appsstats_get_counter(appsstats_obj, counter_name):
    """Retrieves value of a stats counter by its name (aka 'getter')"""
    if not isinstance(appsstats_obj, dict):
        raise TypeError(EX_ARG_NOT_A_DICT)

    if counter_name not in appsstats_obj:
        raise KeyError('Key not found: ' + counter_name)

    return appsstats_obj[counter_name]


def queuesstats_update_stats(qs_obj, queue_name, app_state, app_final_status):
    """Updates an appropriate counter depending on queue name"""
    if not isinstance(qs_obj, dict):
        raise KeyError(EX_ARG_NOT_A_DICT)

    if queue_name not in qs_obj:
        qs_obj[queue_name] = appsstats_init_object()

    appsstats_obj = qs_obj[queue_name]

    appsstats_increase_counter(appsstats_obj, app_state, app_final_status)


_AH_APPDESCRKEYS = ['id', 'state', 'finalStatus', 'applicationType',
                    'queue', 'finishedTime']


def appshistory_insert_app_record(ah_obj, app_descr):
    """Adds application id (+ some other descrs) to the local history"""
    if not isinstance(ah_obj, dict):
        raise KeyError(EX_ARG_NOT_A_DICT)

    if not isinstance(app_descr, dict):
        raise KeyError(EX_ARG_NOT_A_DICT)

    available_keys = set(app_descr.keys())
    expected_keys = set(_AH_APPDESCRKEYS)

    if not expected_keys.issubset(available_keys):
        print 'Missing AppDescr Keys: ', expected_keys.difference(
            available_keys)
        raise ValueError('Invalid input: some app_descr keys are missing')

    app_id = app_descr['id']

    if app_id not in ah_obj:
        ah_obj[app_id] = {
            'state'           : app_descr['state'],
            'finalStatus'     : app_descr['finalStatus'],
            'queue'           : app_descr['queue'].lower(),
            'applicationType' : app_descr['applicationType'],
            'finishedTime'    : app_descr['finishedTime'],
            '_processed'      : 0
        }


def appshistory_remove_old_records(ah_obj, past_timestamp):
    """Removes outdated applications from local history"""
    if not isinstance(ah_obj, dict):
        raise KeyError(EX_ARG_NOT_A_DICT)

    if len(ah_obj) == 0:
        return

    apps_to_remove = []

    for app_id in ah_obj:
        # 'finishedTime' is expressed in miliseconds, we need seconds:
        app_fin_time = int(ah_obj[app_id]['finishedTime']) / 1000
        if app_fin_time < int(past_timestamp):
            apps_to_remove.append(app_id)

    for app_id in apps_to_remove:
        del ah_obj[app_id]

    return len(apps_to_remove)


def appshistory_update_queue_stats(ah_obj, qs_obj):
    """Walks local history to process freshly added applications"""
    if not isinstance(ah_obj, dict):
        raise KeyError(EX_ARG_NOT_A_DICT)
    if not isinstance(qs_obj, dict):
        raise KeyError(EX_ARG_NOT_A_DICT)

    processed_apps_qty = 0

    for app_id in ah_obj:
        app = ah_obj[app_id]
        if app['_processed'] == 0:
            queue_name = app['queue']
            app_state = app['state']
            app_final_status = app['finalStatus']
            queuesstats_update_stats(qs_obj, queue_name, app_state,
                                     app_final_status)
            app['_processed'] = 1
            processed_apps_qty += 1

    return processed_apps_qty


def localvars_get(lv_obj, key):
    """Retrieves key:value pair from script state file"""
    if not isinstance(lv_obj, dict):
        raise KeyError(EX_ARG_NOT_A_DICT)

    return lv_obj.get(key, None)


def localvars_set(lv_obj, key, val):
    """Stores key:value pair into script state file"""
    if not isinstance(lv_obj, dict):
        raise KeyError(EX_ARG_NOT_A_DICT)

    lv_obj[key] = val


def scriptstate_load_from_file(ss_obj, fname):
    """Loads script state left from previous run"""
    with open(fname, 'r') as f_obj:
        try:
            loaded_script_state = json.load(f_obj)
        except ValueError as ex:
            print 'Exception: ', fname, ':', ex
            return

    if not isinstance(loaded_script_state, dict):
        print 'Invalid data in', fname
        return

    # before copying, ensure that object exists and of proper type
    for state_key in ['localVars', 'appsHistory', 'queuesStats']:
        if state_key in loaded_script_state:
            state_val = loaded_script_state[state_key]
            if isinstance(state_val, dict):
                ss_obj[state_key] = state_val


def scriptstate_save_to_file(ss_obj, fname):
    """Saves collected state for the next invocation"""
    with open(fname, 'w') as f_obj:
        json.dump(ss_obj, f_obj)


def scriptstate_jump_to(ss_obj, tree_path):
    """Walks the path through the tree structure"""
    if tree_path is None:
        return ss_obj
    if not isinstance(tree_path, list):
        raise ValueError(EX_ARG_NOT_A_LIST)

    subtree = ss_obj

    while len(tree_path) > 0:
        if not isinstance(subtree, dict):
            print 'Cannot process ', tree_path[0]
            return subtree
        if tree_path[0] not in subtree:
            raise KeyError('Invalid node ' + tree_path[0])

        subtree = subtree.pop(tree_path.pop(0))

    return subtree


def scriptstate_safe_jump_to(ss_obj, tree_path):
    """Tries to reach the leaf of the tree structure"""
    try:
        leaf = scriptstate_jump_to(ss_obj, tree_path)
    except Exception:   # pylint: disable=broad-except
        # This function is involved in creating output for zabbix-agent
        # userparameters scripts.  Any nontrivial output (including
        # any kind of exceptions) is useless, thus, exceptions are
        # suppressed and ZBX_UNSUPPORTED is reported:
        zbx_unsupp_exit()
        ## NOTREACHED ##

    if not isinstance(leaf, (int, long, basestring)):
        zbx_unsupp_exit()
        ## NOTREACHED ##

    return leaf


def _yarnrm_poll_cluster_apps_api(base_url, finished_time):
    """Requests data (including apps list) from the YARN RM API"""
    finished_time_ms = str(int(finished_time) * 1000)
    hdrs = {'Accept'       : 'application/json'}
    pars = {'states'       : 'failed,killed,finished',
            'deSelects'    : 'resourceRequests',
            'finishedTimeBegin' : finished_time_ms
           }
    url = base_url + '/ws/v1/cluster/apps'

    # Query external resource with HTTP GET
    try:
        reply = requests.get(url, headers=hdrs, params=pars)
        if reply.status_code != requests.codes.ok:
            print 'Status Code:', reply.status_code
            print 'HTTP request failed'
            exit(1)
    except requests.exceptions.RequestException as ex:
        print 'RequestException: ', ex
        print 'An exception occured while querying external resource'
        exit(1)

    # Verify reply length
    if len(reply.text) == 0:
        print 'HTTP answer len:', len(reply.text)
        print 'Nothing to process: got zero length response'
        exit(1)

    # Treat response as JSON
    try:
        json_response = reply.json()
    except ValueError as ex:
        print 'ValueError Exception: ', ex
        exit(1)

    return json_response


# (c) https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ \
#      ResourceManagerRest.html#Cluster_Applications_API
#-----------------------------------------------------------------------
#{
#  "apps": {
#    "app": [
#      {
#        "id": "application_1519830322804_0388",
#        ...
#-----------------------------------------------------------------------
#   An exact structure is expected here ^, thus I step two levels in
# depth to reach a list of applications:
def _yarnrm_extract_apps_list(yarnrm_resp):
    """Extracts apps list skipping other info from YARN RM response"""
    list_of_apps = []
    if isinstance(yarnrm_resp, dict):
        js2 = yarnrm_resp.pop('apps')
        if isinstance(js2, dict):
            list_of_apps = js2.pop('app')
            if not isinstance(list_of_apps, list):
                raise ValueError(EX_UNEXPECTED_STRUCT)
    else:
        raise TypeError(EX_ARG_NOT_A_DICT)

    return list_of_apps


def yarnrm_get_finalized_apps_list(base_url, finished_time):
    """Retrieves handy list of applications from YARN RM"""
    yarnrm_resp = _yarnrm_poll_cluster_apps_api(base_url, finished_time)
    apps_list = _yarnrm_extract_apps_list(yarnrm_resp)

    return apps_list

def config_read_from(fname, script_config):
    """Reads key:value pairs from an external file"""
    config_parser = ConfigParser.RawConfigParser()
    config_parser.read(fname)

    for key in script_config:
        val = config_parser.get('global', key)
        script_config[key] = val


# Script's modes of operation
OP_POLL = 'poll'
OP_PRINT = 'print'
OP_DUMP = 'dump'
OP_MODES = (OP_POLL, OP_PRINT, OP_DUMP)


def main():
    """I'm main(), just main()"""
    # The following values should be supplied by script's config file:
    cfg = {
        'baseurl'        : '',
        'state_filename' : '',
        'keep_history'   : ''
    }

    script_state = {
        'localVars'   : {},
        'appsHistory' : {},
        'queuesStats' : {}
    }

    config_read_from('yarnappstats.cfg', cfg)

    current_time = int(time.time())

    # Remove script name from ARGV
    sys.argv.pop(0)

    if len(sys.argv) == 0:
        print 'missing command'
        exit(1)

    cmd = sys.argv.pop(0)
    if cmd not in OP_MODES:
        print 'unknown command, choose from ', OP_MODES
        exit(1)

    # Load saved script state; missing state file should not break the run
    try:
        scriptstate_load_from_file(script_state, cfg['state_filename'])
    except IOError as ex:
        if cmd == OP_PRINT:
            zbx_unsupp_exit()
            ## NOTREACHED ##
        else:
            print 'Exception: ', ex

    appshistory_obj = script_state['appsHistory']
    queuesstats_obj = script_state['queuesStats']
    localvars_obj = script_state['localVars']

    if cmd != OP_POLL:
        # create and fill out fake variable 'lastpoll_ago':
        lastpoll_ago = current_time - int(
            localvars_get(localvars_obj, 'lastpoll_at'))
        localvars_set(localvars_obj, 'lastpoll_ago', str(lastpoll_ago))

    if cmd == OP_PRINT:
        # Zabbix-interface to script state
        ptr = scriptstate_safe_jump_to(script_state, sys.argv)
        print ptr

    elif cmd == OP_DUMP:
        print '# Debug interface'
        ptr = scriptstate_jump_to(script_state, sys.argv)
        print json.dumps(ptr, indent=4, sort_keys=True)

    elif cmd == OP_POLL:
        timestamp_in_past = current_time - int(cfg['keep_history'])
        list_of_apps = yarnrm_get_finalized_apps_list(cfg['baseurl'],
                                                      timestamp_in_past)
        for app in list_of_apps:
            appshistory_insert_app_record(appshistory_obj, app)

        removed_apps_qty = appshistory_remove_old_records(
            appshistory_obj, timestamp_in_past)

        added_apps_qty = appshistory_update_queue_stats(
            appshistory_obj, queuesstats_obj)

        if 'verbose' in sys.argv:
            print 'appsHistory: added ', added_apps_qty,
            print ', removed ', removed_apps_qty
        localvars_set(localvars_obj, 'lastpoll_at', str(current_time))
        scriptstate_save_to_file(script_state, cfg['state_filename'])

if __name__ == '__main__':
    main()
