import bcrypt
import os
import hashlib
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


CLUSTER_HOSTS = os.getenv('CLUSTER_HOSTS', 'localhost').split(',')  # Fetch from environment variable
CASSANDRA_PORT = 9042
KEYSPACE = 'demo'
TABLE = 'users'
cluster = Cluster(contact_points=CLUSTER_HOSTS, port=CASSANDRA_PORT)
session = cluster.connect()
session.execute(f""" 
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 3 }}
""")
session.set_keyspace(KEYSPACE)
session.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        user_id text PRIMARY KEY,
        username 
        password text,
        role text,
        permissions set<text>,
        bird_ids text,
        created_at timestamp
    )
""")

def get_user_hash(username):
    return hashlib.md5(username.encode()).hexdigest()

# Insert users: admin, birdtracker, bird1, bird2, bird3


# Admin with full permissions
session.execute(f"""
    INSERT INTO {TABLE} (user_id, username, password, role, permissions, bird_ids, created_at)
    VALUES ('{get_user_hash('admin1')}', 'admin1', '{bcrypt.hashpw('admin_pass'.encode('utf-8'), bcrypt.gensalt()).decode()}', 'admin', {{'read_bird', 'update_bird', 'admin_panel'}}, null, toTimestamp(now()))
""")

# Tracker with permissions to read birds
session.execute(f"""
    INSERT INTO {TABLE} (user_id, username, password, role, permissions, bird_ids, created_at)
    VALUES ('{get_user_hash('tracker_levi')}', 'tracker_levi', '{bcrypt.hashpw('track123'.encode('utf-8'), bcrypt.gensalt()).decode()}', 'tracker', {{'read_bird'}}, null, toTimestamp(now()))
""")

# Bird1 users with permissions to update their own bird data
session.execute(f"""
    INSERT INTO {TABLE} (user_id, username, password, role, permissions, bird_ids, created_at)
    VALUES ('{get_user_hash('bird1')}', 'bird1', '{bcrypt.hashpw('bird1pass'.encode('utf-8'), bcrypt.gensalt()).decode()}', 'bird', {{'update_bird'}}, 'c2a6999f1e03c8446e8bd8ffae7db61f', toTimestamp(now()))
""")
# Bird2 users with permissions to update their own bird data
session.execute(f"""
    INSERT INTO {TABLE} (user_id, username, password, role, permissions, bird_ids, created_at)
    VALUES ('{get_user_hash('bird2')}', 'bird2', '{bcrypt.hashpw('bird2pass'.encode('utf-8'), bcrypt.gensalt()).decode()}', 'bird', {{'update_bird'}}, 'c2a6999f1e03c8446e8bd8ffae7db61f', toTimestamp(now()))
""")

# Bird3 users with permissions to update their own bird data
session.execute(f"""
    INSERT INTO {TABLE} (user_id, username, password, role, permissions, bird_ids, created_at)
    VALUES ('{get_user_hash('bird3')}', 'bird3', '{bcrypt.hashpw('bird3pass'.encode('utf-8'), bcrypt.gensalt()).decode()}', 'bird', {{'update_bird'}}, 'c2a6999f1e03c8446e8bd8ffae7db61f', toTimestamp(now()))
""")



def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

def check_password(hashed_password, password):
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password)

def get_user (username, password):
    if(username and password):
        user_id = get_user_hash(username)
        select_stmt = session.prepare(f"""
            SELECT * FROM {TABLE} WHERE user_id = ?
        """)
        row = session.execute(select_stmt, (user_id,)).one()
        if row and check_password(row.password, password):
            # add hash to check permissions
            user_data = {
                'user_id': row.user_id,
                'username': row.username,
                'role': row.role,
                'permissions': row.permissions,
                'bird_ids': row.bird_ids,
                'created_at': row.created_at            
            }
            permissions_hash = bcrypt.hashpw(user_data.encode('utf-8'), bcrypt.gensalt()).decode()

            user = {
                'user_id': row.user_id,
                'username': row.username,
                'role': row.role,
                'permissions': row.permissions,
                'bird_ids': row.bird_ids,
                'created_at': row.created_at,
                'permissions_hash': permissions_hash
            }
            return
        else:
            return None
    else:
        print("Username and password cannot be empty.")
        return None

def check_permissions(user, permission):
    user_data = {
        'user_id': user['user_id'],
        'username': user['username'],
        'role': user['role'],
        'permissions': user['permissions'],
        'bird_ids': user['bird_ids'],
        'created_at': user['created_at'],
    }
    permissions_hash = user['permissions_hash']
    if bcrypt.checkpw(permission.encode('utf-8'), permissions_hash.encode('utf-8')) and permission in user_data['permissions']:
        return True
    return False
    

