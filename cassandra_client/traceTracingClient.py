import os
import time
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandrabirdClientApp import get_bird_hash, get_stmt_bird_data

CLUSTER_HOSTS = os.getenv('CLUSTER_HOSTS', 'localhost').split(',')
CASSANDRA_PORT = 9042
KEYSPACE = 'birds_app'
TABLE = 'birds'

cluster = Cluster(contact_points=CLUSTER_HOSTS, port=CASSANDRA_PORT)
session = cluster.connect()
session.set_keyspace(KEYSPACE)

timeout = 10  # seconds, for waiting for trace

names = [f'BIRD{I}' for I in range(1, 21)]

with open("bird_data_trace_log.txt", "w") as data_log, open("trace_data_log.txt", "w") as trace_log:
    for round_num in range(10):
        data_log.write(f"\n=== Round {round_num + 1} ===\n")
        trace_log.write(f"\n=== Round {round_num + 1} ===\n")
        print(f"\n=== Round {round_num + 1} ===")
        for name in names:
            bird_id = get_bird_hash(name)
            bound_select = get_stmt_bird_data(bird_id)
            bound_select.consistency_level = ConsistencyLevel.ONE

            # Execute the query with tracing enabled
            result = session.execute(bound_select, (bird_id,), trace=True)
            rows = list(result)
            row = rows[0] if rows else None

            # Wait for trace to be available (up to timeout seconds)
            trace = None
            start_time = time.time()
            while not trace and (time.time() - start_time < timeout):
                trace = result.get_query_trace()
                if not trace:
                    time.sleep(0.1)

            # Log bird data
            if rows:
                data_log.write(f"Bird ID: {bird_id}, day: {row.day}, Timestamp: {row.timestamp}, name: {name}, Latitude: {row.latitude}, Longitude: {row.longitude}\n")
                print(f"Bird ID: {bird_id}, day: {row.day}, Timestamp: {row.timestamp}, name: {name}, Latitude: {row.latitude}, Longitude: {row.longitude}")
            else:
                data_log.write(f"Bird ID: {bird_id}: No data\n")
                print(f"Bird ID: {bird_id}: No data")

            # Log trace data
            if trace:
                trace_log.write(f"Bird ID: {bird_id}, name: {name}\n")
                trace_log.write(f"  Coordinator: {trace.coordinator}\n")
                trace_log.write(f"  Request Type: {trace.request_type}\n")
                trace_log.write(f"  Events:\n")
                for event in trace.events:
                    trace_log.write(f"    Time: {event.datetime}, Source: {event.source}, Desc: {event.description}\n")
            else:
                trace_log.write(f"Bird ID: {bird_id}, name: {name} - No trace available\n")
        time.sleep(60)

session.shutdown()
cluster.shutdown()
print("Data retrieval complete. Check 'bird_data_log.txt' and 'trace_data_log.txt' for results.")
