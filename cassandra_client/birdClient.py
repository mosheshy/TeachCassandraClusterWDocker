from cassandrabirdClientApp import insert_bird_data, get_bird_hash
import time
import random
from datetime import datetime, timedelta

if __name__ == "__main__":
    names = [f'BIRD{I}' for I in range(1, 21)]

    bird_locations = {name: (random.uniform(-90, 90), random.uniform(-180, 180)) for name in names}

    start_time = datetime.utcnow()
    
    for i in range(20):
        
        print(f"\n=== Round {i + 1} ===")
        for name in names:
            print(f"Data for {name}:")
            lat, lon = bird_locations[name]
            latitude = lat + random.uniform(-0.01, 0.01)
            longitude = lon + random.uniform(-0.01, 0.01)
            timestamp = datetime.utcnow() 
            bird_id = get_bird_hash(name)
            print(f"Timestamp: {timestamp}, Latitude: {latitude}, Longitude: {longitude}")
            insert_bird_data(bird_id, timestamp,name, latitude, longitude)            
        time.sleep(10)  # Simulate a delay for each insert
    