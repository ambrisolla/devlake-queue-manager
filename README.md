# devlake-queue-manager

<b>devlake-queue-manager</b> is a tools to keep <b><a target="_blank" href="https://devlake.apache.org/">Devlake</a></b> data up-to-date. 

## Why
Devlake already contains a cron service to manage all <b><a target="_blank" href="https://devlake.apache.org/docs/Overview/KeyConcepts#blueprint">blueprints</a></b> automatically,but in my case, it is necessary to disable cron because there are too many blueprints and managing cron became a chore.

# About Devlake
<a href="https://github.com/apache/incubator-devlake" target="_blank"><b>Apache DevLake</b></a> is an open-source dev data platform to ingest, analyze, and visualize the fragmented data from DevOps tools, extracting insights for engineering excellence, developer experience, and community growth.

# How to use
```bash
$ pip install requests
$ export DEVLAKE_ENDPOINT="http://your-devlake-server:8080"
$ python run.py
```
