from cassandrabirdClientApp import get_bird_token, get_bird_hash
import re
import os
from collections import defaultdict

""" def read_node_tokens_from_ring(filename='ring.txt'):
    node_tokens = []
    try:
        # Check if file exists
        if not os.path.exists(filename):
            print(f"Error: File '{filename}' not found!")
            # Return default test values since file is missing
            return [(0, "172.20.0.2"), (3074457345618258602, "172.20.0.3"), (6148914691236517205, "172.20.0.4")]
            
        with open(filename, 'r') as f:
            for line in f:
                match = re.search(r'(\d+\.\d+\.\d+\.\d+).*?([-]?\d+)', line)
                if match:
                    ip = match.group(1)
                    token = int(match.group(2))
                    node_tokens.append((token, ip))
        
        # Check if we found any valid entries
        if not node_tokens:
            print(f"Warning: No valid node tokens found in '{filename}'")
            # Return default test values
            return [(0, "172.20.0.2"), (3074457345618258602, "172.20.0.3"), (6148914691236517205, "172.20.0.4")]
            
        node_tokens.sort(key=lambda x: x[0])
        return node_tokens
    except Exception as e:
        print(f"Error reading node tokens: {e}")
        return [(0, "172.20.0.2"), (3074457345618258602, "172.20.0.3"), (6148914691236517205, "172.20.0.4")]
    """
    
def read_node_tokens_from_ring(filename='ring.txt'):
    node_tokens = []
    with open(filename, 'r') as f:
        for line in f:
            match = re.match(r'^\s*(\d+\.\d+\.\d+\.\d+).*?([\-]?\d+)\s*$', line)
            match = re.match(r'^\s*(\d+\.\d+\.\d+\.\d+).*?([\-]?\d+)\s*$', line)
            if match:
                ip = match.group(1)
                token = int(match.group(2))
                node_tokens.append((token, ip))
    node_tokens.sort(key=lambda x: x[0])
    return node_tokens

# Remove the first duplicate function and keep only this version
def find_node_for_token(bird_id, token, node_tokens):
    """
    Finds which node (IP) is responsible for the given bird_id (token), 
    based on Cassandra ring token assignment.
    """
    if not node_tokens:
        print("Error: Empty node_tokens list!")
        return "unknown", 0
        
    for t, ip in node_tokens:
        if t >= token:
            print(f"{bird_id} (Token: {token}) --> Node: {ip} (Node Token: {t})")
            return ip, t
    
    # Wrap around to the first token if needed
    ip, t = node_tokens[0]
    print(f"{bird_id} (Token: {token}) --> Node: {ip} (Node Token: {t}) [wrap around]")
    return ip, t


if __name__ == "__main__":
    # Read tokens with error handling
    node_tokens = read_node_tokens_from_ring('ring.txt')
    ip_map = defaultdict(list)

    print(f"Found {len(node_tokens)} nodes in the ring")

    # Fix uppercase I to lowercase i
    names = [f'BIRD{i}' for i in range(1, 21)]
    ip_partitions_count = defaultdict(int)


    for name in names:
        bird_id = get_bird_hash(name)
        token = get_bird_token(bird_id)
        node_ip, node_token = find_node_for_token(bird_id, token, node_tokens)
        ip, node_token = find_node_for_token(bird_id, token, node_tokens)
        ip_map[ip].append((name, bird_id, token, node_token))
        ip_partitions_count[ip] += 1
        print(f"name: {name}, Bird ID: {bird_id}, Token: {token}")
        
    print("\nNode IPs and their assigned birds:")
    for ip, count in ip_partitions_count.items():
        print(f"Node {ip} --> {count} partitions (birds)")
        
    for ip, birds in ip_map.items():
        print(f"\nNode {ip} has the following birds:")
        for name, bird_id, token, node_token in birds:
            print(f"ip: {ip} ,  Name: {name}, Bird ID: {bird_id}, Token: {token}, Node Token: {node_token}")
    print("\nData retrieval complete.")