from cassandrabirdClientApp import get_bird_hash, get_bird_last_location, insert_bird_data

import random
from datetime import datetime
import time
from collections import defaultdict

if __name__ == "__main__":
    # Example usage
    birdsNames = {'BIRD3', 'BIRD4', 'BIRD9', 'BIRD11', 'BIRD14','BIRD17'}
    
    birdsMap = defaultdict(list)

    for birdName in birdsNames:
        bird_id = get_bird_hash(birdName)
        bird = get_bird_last_location(bird_id)
        print (f"bird: {bird}, bird_id: {bird_id}")
        birdsMap[birdName].append(bird)

    time.sleep(100)
    for birdName in birdsNames:
        bird_id = get_bird_hash(birdName)
        bird = get_bird_last_location(bird_id)
        print(f"Retrieved data for {birdName} (ID: {bird_id}): {bird}")
        #compre bird to birdsMap[birdName] by all the fields and show the differences
        if bird != birdsMap[birdName][0]:
            print(f"Data mismatch for {birdName} (ID: {bird_id}): Expected {birdsMap[birdName][0]}, but got {bird}")
        else:
            print(f"Data for {birdName} (ID: {bird_id}) matches expected values.")


    
""" 
    for birdName in birdsNames:
        bird_id = get_bird_hash(birdName)
        insert_bird_data(bird_id, datetime.utcnow(), birdName, random.uniform(-90, 90), random.uniform(-180, 180))
        print(f"Data for {birdName} (ID: {bird_id}) inserted successfully ")
    print("Data insertion complete.") 
"""
    