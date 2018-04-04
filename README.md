# yarnappstats.py

The target:
https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Applications_API

The script is aimed to poll YARN Resource Manager Cluster Applications API and collect statistics about finalized ('failed', 'killed', 'finished') applications.

The script has three modes of operation:
* **poll** -- An actual querying of information from YARN RM is performed, a retrieved list of applications(jobs) is processed, statistics is updated (which is hid in a local state file).
* **print** -- The local state file is queried for an exact metric.
* **dump** -- For debugging purposes: allows examining of the local state file contents.

## Config file
The script expects to find a `yarnappstats.cfg` in its CWD. The contents is as follows:
```
[global]
baseurl = http://localhost:8088
state_filename = state.json
keep_history = 86400
```
Where:
* **_baseurl_** - YARN ResourceManager interface.
* **_state_filename_** - the local state file (consider hiding it under `/var` or `/tmp`).
* **_keep_history_** - check status of applicataions for the past **_keep_history_** seconds.

## Expected usage:
1. A cronjob runs `yarnappstats.py poll` regularly.
2. A Zabbix-agent polls for the individual metrics by running `yarnappstats.py print <...>`.

## Examples:
```sh
% ./yarnappstats.py poll
appsHistory: added  1 , removed  0
```
Note: `appsHistory: added ...` may be removed soon.

```sh
% ./yarnappstats.py dump
# Debug interface
{
    "appsHistory": {
        "application_1519830322804_0001": {
            "_processed": "1",
            "applicationType": "SPARK",
            ...
    "queuesStats": {
        "etl": {
            "failed": 3,
            ...
 ```
That may produce large amounts of output, it's better be refined to exact node name, i.e.:
```sh
% ./yarnappstats.py dump queuesStats
# Debug interface
{
    "etl": {
        "failed": 3,
        "finished.failed": 0,
        "finished.killed": 0,
        "finished.succeeded": 1,
        ...
```
... or, even narrower:
```sh
% ./yarnappstats.py dump queuesStats etl
# Debug interface
{
    "failed": 3,
    "finished.failed": 0,
    "finished.killed": 0,
    "finished.succeeded": 1,
    "finished.undefined": 0,
    "killed": 0,
    "other": 0
}

% ./yarnappstats.py dump queuesStats etl finished.succeeded
# Debug interface
1
```
Note: The last run helps on guessing for zabbix-agent userparameter script agruments, `dump` is to be substituted with `print`, e.g.:
```sh
% ./yarnappstats.py print queuesStats etl finished.succeeded
1
```
The Zabbix-agent userparameter config file might be something like:
```sh
UserParameter=my.fancy.zbx.item.key[*],yarnappstats.py print queuesStats $1 $2
```
Where:
* **$1** - is a queue name,
* **$2** - is a stats counter name


----
# yarnpoll.py

The target:
https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Information_API

Not documented (RTFS) + to be deprecated.
