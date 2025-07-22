import os

from cassandrabirdClientApp import get_bird_data
import time as t
from cassandra.cluster import Cluster
CLUSTER_HOSTS = os.getenv('CLUSTER_HOSTS', 'localhost').split(',')  # Fetch from environment variable
CASSANDRA_PORT = 9042


def get_bird_data(bird_id):
    result = session.execute(f"""
                SELECT * FROM {TABLE} WHERE bird_id=%s LIMIT 1
            """, (bird_id,))
    return result.one()
    

cluster  = Cluster(contact_points=CLUSTER_HOSTS, port=CASSANDRA_PORT)
session = cluster.connect('demo')
TABLE = 'birds'

bird_ids = [f'BIRD{I}' for I in range(1, 11)]

with open('bird_tracler_log.txt', 'w') as file: 
    for round in range(20):
        file.write(f"Round {round + 1}:\n")
        for bird_id in bird_ids:
            bird_data = get_bird_data(bird_id)
            if bird_data:
                file.write(f"Bird ID: {bird_id}, Timestamp: {bird_data.timestamp}, Latitude: {bird_data.latitude}, Longitude: {bird_data.longitude}\n")
            else:
                file.write(f"No data found for Bird ID: {bird_id}\n")
        t.sleep(10)  # Simulate a delay between rounds
    print("Data retrieval complete. Check 'bird_tracler_log.txt' for results.")
    