#!/usr/bin/env python3
import json
import socket
import time
import random
import string

# Configuration for the three nodes.
# Change the ports as needed (they should match your PeerServer's PeerAddr).
NODES = {
    "leader": ("localhost", 9000),
    "follower1": ("localhost", 9002),
    "follower2": ("localhost", 9003)
}

# Helper function to send a message over TCP and receive the JSON response.
def send_tcp_message(addr, message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(addr)
    s.sendall(json.dumps(message).encode("utf-8"))
    # Signal that we're done sending data.
    s.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    s.close()
    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        print("Error decoding response:", e)
        return None


# Query the current lamport clock from a given node.
def get_lamport(node_name):
    addr = NODES[node_name]
    msg = {
        "Type": 11,
        "Data": {},
        "Timestamp": 0,
        "Metadata": {"Sender": ""}
    }
    response = send_tcp_message(addr, msg)
    lamport = response.get("lamport") if response else None
    print(f"[{node_name}] Lamport clock: {lamport}")
    return lamport

# Simulate a store request.
def send_store_request(target_node, user_id):
    addr = NODES[target_node]
    # Create a random message ID and payload.
    msg_id = random.getrandbits(32)
    payload = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    so = {
        "user_id": user_id,
        "user_message_id": msg_id,
        "data": payload
    }
    msg = {
        "Type": 2,  # StoreObject
        "Data": so,
        "Timestamp": 0,  # Initially 0; the server(s) will update it.
        "Metadata": {"Sender": ""}
    }
    response = send_tcp_message(addr, msg)
    print(f"Store request to {target_node} for user {user_id} with message id {msg_id} response: {response}")

def display_all_lamports(label):
    print(f"\n=== {label} ===")
    for node in NODES:
        get_lamport(node)

def main():
    # Initial lamport clocks
    display_all_lamports("Initial Lamport Clocks")
    
    # Pause briefly
    time.sleep(0.2)
    
    # Transaction 1: Send store request to follower1 (which should forward to leader)
    print("\n--- Transaction 1: Store via follower1 ---")
    send_store_request("follower1", 1)
    time.sleep(0.3)
    display_all_lamports("After Transaction 1")
    
    # Transaction 2: Send store request directly to leader.
    print("\n--- Transaction 2: Store via leader ---")
    send_store_request("leader", 2)
    time.sleep(0.3)
    display_all_lamports("After Transaction 2")
    
    # Transaction 3: Send store request to follower2 (which should forward to leader)
    print("\n--- Transaction 3: Store via follower2 ---")
    send_store_request("follower2", 3)
    time.sleep(0.3)
    display_all_lamports("After Transaction 3")
    
    print("\nTest completed.")

if __name__ == '__main__':
    main()
