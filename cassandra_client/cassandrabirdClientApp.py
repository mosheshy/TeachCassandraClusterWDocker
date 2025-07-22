import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from datetime import datetime 
import hashlib
import random

# Configuration
CLUSTER_HOSTS = os.getenv('CLUSTER_HOSTS', 'localhost').split(',')  # Fetch from environment variable
CASSANDRA_PORT = 9042
KEYSPACE = 'demo'
TABLE = 'birds'
cluster = Cluster(contact_points=CLUSTER_HOSTS, port=CASSANDRA_PORT)
session = cluster.connect()
 
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 3 }}
""")
session.set_keyspace(KEYSPACE)
session.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    bird_id text,
                    day date,
                    timestamp timestamp,
                    name text,
                    latitude double,
                    longitude double,
                    PRIMARY KEY (bird_id, timestamp)                    
                )WITH CLUSTERING ORDER BY (timestamp DESC)
""")

def insert_bird_data(bird_id,timestamp, name, latitude, longitude):
    insert_stmt = session.prepare(f"""
        INSERT INTO {TABLE} (bird_id, day, timestamp, name, latitude, longitude)
        VALUES (?, ?, ?, ?, ?)  
    """)
    session.execute(insert_stmt, (bird_id, timestamp.date(), timestamp, name, latitude, longitude))
    print(f"Inserted bird data: {bird_id} in {timestamp.date()} at {timestamp}, name: {name}, latitude: {latitude}, longitude: {longitude}")

def get_bird_data(bird_id):
    select_stmt = session.prepare(f"""
        SELECT timestamp, latitude, longitude FROM {TABLE} WHERE bird_id = ?
        order by timestamp DESC
    """)
    rows = session.execute(select_stmt, (bird_id,))
    for row in rows:
        print(f"Bird ID: {bird_id}, Timestamp: {row.timestamp}, Latitude: {row.latitude}, Longitude: {row.longitude}")

def get_bird_last_location(bird_id):
    select_stmt = session.prepare(f"""
        SELECT timestamp, latitude, longitude FROM {TABLE} WHERE bird_id = ?
        ORDER BY timestamp DESC LIMIT 1
    """)
    row = session.execute(select_stmt, (bird_id,)).one()
    if row:
        print(f"Last location of Bird ID: {bird_id} - Timestamp: {row.timestamp}, Latitude: {row.latitude}, Longitude: {row.longitude}")
    else:
        print(f"No location data found for Bird ID: {bird_id}")
    return row.timestamp, row.latitude, row.longitude if row else None

def get_bird_location_history(bird_id, start_time, end_time):
    select_stmt = session.prepare(f"""
        SELECT timestamp, latitude, longitude FROM {TABLE} 
        WHERE bird_id = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp DESC
    """)
    rows = session.execute(select_stmt, (bird_id, start_time, end_time))
    for row in rows:
        print(f"Bird ID: {bird_id}, Timestamp: {row.timestamp}, Latitude: {row.latitude}, Longitude: {row.longitude}")
        
def delete_bird_data(bird_id, timestamp):
    delete_stmt = session.prepare(f"""
        DELETE FROM {TABLE} WHERE bird_id = ? AND timestamp = ?
    """)
    session.execute(delete_stmt, (bird_id, timestamp))
    print(f"Deleted bird data: {bird_id} at {timestamp}")
    

def delete_bird_data(bird_id):
    delete_stmt = session.prepare(f"""
        DELETE FROM {TABLE} WHERE bird_id = ?
    """)
    session.execute(delete_stmt, (bird_id,))
    print(f"Deleted bird data: {bird_id}")

def get_bird_token(bird_id):
    select_stmt = session.prepare(f"""
        SELECT token(bird_id) AS token_val FROM {TABLE} WHERE bird_id = ?
    """)
    row = session.execute(select_stmt, (bird_id,)).one()
    if row:
        print(f"Bird ID: {bird_id}, Token: {row.token_val}")
        return row.token_val
    else:
        print(f"No token found for Bird ID: {bird_id}")
        return None

def get_bird_hash(bird_name):
    
    return hashlib.md5(bird_name.encode()).hexdigest()

def get_stmt_bird_data(bird_id):
    return session.prepare(f"""
        SELECT * FROM {TABLE} WHERE bird_id = ? LIMIT 1
    """)
    
if __name__ == "__main__":
    # Example usage
    session.set_keyspace(KEYSPACE)
    
    




    
    names = [f'BIRD{I}' for I in range(1, 21)]
    for name in names:
        hashed_id = get_bird_hash(name)
        print(f"{name} -> {hashed_id}")
        get_bird_token(hashed_id)
        #delete_bird_data(bird_id)
        #get_bird_data
    #print("All bird data deleted.")
