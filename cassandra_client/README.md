# Bird Tracking System - Cassandra Distributed Exercise

## Students:
- moshe shayevitz (036452167)

## Environment:
- OS:  Debian 12
- Python version: 3.9
- Cassandra version: 4.1 (with cqlsh)
- Python packages: cassandra-driver, bcrypt, hashlib, etc.

## Files submitted:
- ex2_birds_version2.docx (main answers file)
- cassandrabirdClientApp.py (Cassandra client operations)
- bird_history.py
- cronjob.txt 
- Dockerfile
- birdClient.py
- TrackerClient.py
- traceTracingClient.py
- bird_data_trace_log.txt
- bird_tracking_log.txt
- trace_data_log.txt
- ring.txt
- ringMap.py
- map.text
- test_bird_failer.py
- test_bird_failer.txt
- users.py

- bird_data_trace_log
- users.py (user management/authentication module)
- ringMap.py (token-ring processing script)
- test_bird_failer.py (node failure simulation client)

## Running the code:
```
add cronjob : 0 3 * * * root /usr/bin/python3 /app/bird_history.py >> /var/log/cron.log 2>&1
```

Set environment variable for Cassandra hosts (if not localhost):
```bash
export CLUSTER_HOSTS=127.0.0.1
```

github:  : [TeachCassandraClusterWDocker: mosheshay ](https://github.com/mosheshy/TeachCassandraClusterWDocker)