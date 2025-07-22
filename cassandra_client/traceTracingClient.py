import os
import time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandrabirdClientApp import  get_bird_hash


CLUSTER_HOSTS = os.getenv('CLUSTER_HOSTS', 'localhost').split(',')
CASSANDRA_PORT = 9042
KEYSPACE = 'demo'
TABLE = 'birds'

cluster = Cluster(contact_points=CLUSTER_HOSTS, port=CASSANDRA_PORT)
session = cluster.connect()
session.set_keyspace(KEYSPACE)

select_stmt = session.prepare(f"""
    SELECT * FROM {TABLE} WHERE bird_id = ? LIMIT 1
""")

names = [f'BIRD{I}' for I in range(1, 21)]

with open("tracker_trace_log.txt", "w") as log_file:
    for round_num in range(20):
        log_file.write(f"\n=== Round {round_num + 1} ===\n")
        print(f"\n=== Round {round_num + 1} ===")
        for name in names:
            bird_id = get_bird_hash(name)
            bound_select = select_stmt.bind((bird_id,))
            bound_select.trace = True
            result = session.execute(bound_select)
            rows = list(result)
            if rows:
                row = rows[0]
                log_file.write(f"Bird ID: {bird_id}, Timestamp: {row.timestamp}, name: {name}, Latitude: {row.latitude}, Longitude: {row.longitude}\n")
                print(f"Bird ID: {bird_id}, Timestamp: {row.timestamp}, name: {name}, Latitude: {row.latitude}, Longitude: {row.longitude}")
            else:
                log_file.write(f"Bird ID: {bird_id}: No data\n")
                print(f"Bird ID: {bird_id}: No data")                       
            time.sleep(0.1)
            trace = result.get_query_trace()
            if trace:
                log_file.write(f"  Coordinator: {trace.coordinator}\n")
                log_file.write(f"  Request Type: {trace.request_type}\n")
                log_file.write(f"  Events:\n")
                for event in trace.events:
                    log_file.write(f"    Time: {event.timestamp}, Source: {event.source}, Desc: {event.description}\n")
            else:
                log_file.write("  No trace available\n")
               
        time.sleep(10)

session.shutdown()
cluster.shutdown()
print("Data retrieval complete. Check 'tracker_trace_log.txt' for results.")
