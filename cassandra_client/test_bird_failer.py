from cassandrabirdClientApp import get_bird_hash, get_bird_last_location, insert_bird_data

import random
from datetime import datetime
import time
from collections import defaultdict

if __name__ == "__main__":
    # Example usage
    birdsNames = {'BIRD1', 'BIRD3', 'BIRD4', 'BIRD5', 'BIRD9','BIRD17'}
    
    birdsMap = defaultdict(list)

    for birdName in birdsNames:
        # The `bird_id` variable in the provided Python script is being used to store the unique
        # identifier (hash) of a bird based on its name. This identifier is obtained by calling the
        # `get_bird_hash` function with the bird's name as an argument. The `bird_id` is then used to
        # retrieve the last known location data of the bird by calling the `get_bird_last_location`
        # function with the `bird_id` as a parameter.
        bird_id = get_bird_hash(birdName)
       # The code snippet you provided is performing the following actions:
        bird = get_bird_last_location(bird_id)
        print(f"Retrieved data for {birdName} (ID: {bird_id}): {bird}, name: {bird.name}, day: {bird.day}, timestamp: {bird.timestamp}, Latitude: {bird.latitude}, Longitude: {bird.longitude}")
        birdsMap[birdName].append(bird)
    # turn off the server "cassandra-2" to test the failover
    # The `time` module in Python is being used in the provided code to introduce delays in the script
    # execution. This can be helpful for simulating certain scenarios like server downtime or waiting
    # for data to be available before retrieval.
    print("fetching data after server downtime...  ")
    time.sleep(30)
    for birdName in birdsNames:
        bird_id = get_bird_hash(birdName)
        bird = get_bird_last_location(bird_id)
        print(f"Retrieved data for {birdName} (ID: {bird_id}): {bird}")
        #compre bird to birdsMap[birdName] by all the fields and show the differences
        if bird != birdsMap[birdName][0]:
            print(f"Data mismatch for {birdName} (ID: {bird_id}): Expected {birdsMap[birdName][0]}, but got {bird}")
        else:
            print(f"Data for {birdName} (ID: {bird_id}) matches expected values.")
    # # Insert new data for the bird3 on the server cassandra-2       172.18.0.3 
    bird3_id = get_bird_hash('BIRD3')
    latitude = random.uniform(-90, 90)
    longitude = random.uniform(-180, 180)
    insert_bird_data(bird3_id, datetime.utcnow().date(),datetime.utcnow(), 'BIRD3', latitude, longitude)
    print(f"Inserted new data for BIRD3 (ID: {bird3_id} name: 'BIRD3'): Latitude: {latitude}, Longitude: {longitude}")
    print("Data insertion complete.")
    bird3 = get_bird_last_location(bird3_id)
    print(f"Retrieved data for BIRD3 (ID: {bird3_id} name: 'BIRD3'): {bird3} , Latitude: {bird3.latitude}, Longitude: {bird3.longitude}")
    
    #   i tern on the serveer "cassandra-2" to test the failover
    print("Fetching data after server restart...")
    time.sleep(45)  # Wait for a while to ensure the data is inserted and can be retrieved
    bird_3 = get_bird_last_location(bird3_id)
    print(f"Retrieved data for BIRD3 (ID: {bird3_id} name: 'BIRD3'): {bird_3.name} , Latitude: {bird_3.latitude}, Longitude: {bird_3.longitude}")

        #compare bird_3 to bird3
    if bird_3 != bird3 and (bird_3.latitude != bird3.latitude or bird_3.longitude != bird3.longitude):
        print(f"Data mismatch for BIRD3 (ID: {bird3_id}): Expected {bird3}, but got {bird_3}")
    else:
        print(f"Data for BIRD3 (ID: {bird3_id}) matches expected values.")
        