import os
from cassandra.cluster import Cluster
import datetime

CLUSTER_HOSTS = os.getenv('CLUSTER_HOSTS', 'localhost').split(',')
CASSANDRA_PORT = 9042
KEYSPACE = 'birds_app'
HISTORY_TABLE = 'bird_history'
MAIN_TABLE = 'birds'

cluster = Cluster(contact_points=CLUSTER_HOSTS, port=CASSANDRA_PORT)
session = cluster.connect()

# Create keyspace if it doesn't exist
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 3 }}
""")

session.set_keyspace(KEYSPACE)

# Create history table if it doesn't exist
session.execute(f"""
    CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
        bird_id text,
        day date,
        timestamp timestamp,
        name text,
        latitude double,
        longitude double,
        PRIMARY KEY ((bird_id, day), timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
""")

now = datetime.datetime.now()
day_ago = now - datetime.timedelta(days=1)

# Select old records (older than 24 hours)
old_rows = session.execute(
    f"SELECT * FROM {MAIN_TABLE} WHERE day < %s ALLOW FILTERING", (day_ago.date(),)
)

# Copy to bird_history and delete from birds
for row in old_rows:
    # Insert into history
    session.execute(
        f"INSERT INTO {HISTORY_TABLE} (bird_id, day, timestamp, name, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s)",
        (row.bird_id, row.day, row.timestamp, row.name, row.latitude, row.longitude)
    )
    # Delete from main table
    session.execute(
        f"DELETE FROM {MAIN_TABLE} WHERE bird_id=%s AND day=%s AND timestamp=%s",
        (row.bird_id, row.day, row.timestamp)
    )

session.shutdown()
cluster.shutdown()
