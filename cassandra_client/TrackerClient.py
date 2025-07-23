import os
from cassandrabirdClientApp import get_stmt_bird_data, get_bird_hash
import time as t
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
CLUSTER_HOSTS = os.getenv('CLUSTER_HOSTS', 'localhost').split(',')  # Fetch from environment variable
CASSANDRA_PORT = 9042
KEYSPACE = 'birds_app'



cluster  = Cluster(contact_points=CLUSTER_HOSTS, port=CASSANDRA_PORT)
session = cluster.connect()
session.set_keyspace(KEYSPACE)
TABLE = 'birds'

names = [f'BIRD{I}' for I in range(1, 21)]

with open('bird_tracking_log.txt', 'w') as file: 
    for round in range(10):
        t.sleep(60)  # Simulate a delay between rounds
        file.write(f"Round {round + 1}:\n")
        for name in names:
            bird_id = get_bird_hash(name)
            bound_select = get_stmt_bird_data(bird_id)            
            bound_select.consistency_level = ConsistencyLevel.ONE
            result = session.execute(bound_select, (bird_id,))
            bird_data = result.one()

            if bird_data:
                print(f"Bird ID: {bird_id}, day: {bird_data.day}, Timestamp: {bird_data.timestamp}, Latitude: {bird_data.latitude}, Longitude: {bird_data.longitude}")
                file.write(f"Bird ID: {bird_id}, day: {bird_data.day}, Timestamp: {bird_data.timestamp}, Latitude: {bird_data.latitude}, Longitude: {bird_data.longitude}\n")
            else:
                print(f"No data found for Bird ID: {bird_id}")
                file.write(f"No data found for Bird ID: {bird_id}\n")
    print("Data retrieval complete. Check 'bird_tracler_log.txt' for results.")
    