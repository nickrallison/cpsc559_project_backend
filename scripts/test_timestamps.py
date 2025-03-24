#!/usr/bin/env python3
# This only tests order in which the updates are applied only on one single node. It sends the request directly to the peer port of the "follower"
# since the metadata points to the leader's address, the peer assumes that it is already coming in from the leader, therefore, this script won't trigger any actions on the leader side/
# make sure only "follower" and "leader" is running on port 8081
import socket
import json
import time
import requests

# Configuration: adjust these if needed.
FOLLOWER_PEER_HOST = "localhost"
FOLLOWER_PEER_PORT = 9002  # Peer port for the follower we want to test.
FOLLOWER_HTTP_URL = "http://localhost:8081"  # HTTP endpoint for the same follower.
LEADER_ADDR = "localhost:9000"  # Used in the message metadata.

def send_peer_message(message):
    """Send a JSON-encoded peer message over TCP and print the response."""
    try:
        with socket.create_connection((FOLLOWER_PEER_HOST, FOLLOWER_PEER_PORT)) as sock:
            msg_json = json.dumps(message)
            # Send the JSON followed by a newline.
            sock.sendall(msg_json.encode("utf-8") + b"\n")
            # Receive response (up to 1024 bytes).
            response = sock.recv(1024)
            print(f"Sent message (timestamp {message['Timestamp']}) of type {message['Type']}. Received: {response.decode('utf-8')}")
    except Exception as e:
        print(f"Error sending message with timestamp {message['Timestamp']}: {e}")

def get_stored_objects(user_id):
    """Query the follower's HTTP endpoint to get stored objects for a user."""
    try:
        resp = requests.get(f"{FOLLOWER_HTTP_URL}/objects", params={"userId": user_id})
        if resp.status_code != 200:
            print(f"GET for user {user_id} failed: {resp.status_code} {resp.text}")
            return None
        return resp.json()
    except Exception as e:
        print(f"Exception during GET: {e}")
        return None

def main():
    messages = [
        {
            "Type": 1, 
            "Data": {"user_id": 1, "user_message_id": 3, "data": "third update"},
            "Timestamp": 3,
            "Metadata": {"Sender": LEADER_ADDR, "PeerToAdd": ""}
        },
        {
            "Type": 1, 
            "Data": {"user_id": 1, "user_message_id": 1, "data": "first update"},
            "Timestamp": 1,
            "Metadata": {"Sender": LEADER_ADDR, "PeerToAdd": ""}
        },
        {
            "Type": 1, 
            "Data": {"user_id": 1, "user_message_id": 2, "data": "second update"},
            "Timestamp": 2,
            "Metadata": {"Sender": LEADER_ADDR, "PeerToAdd": ""}
        }
    ]

    print("Sending out-of-order peer messages to follower (simulating leader updates):")
    # Send messages in the order: timestamp 3, then 1, then 2.
    for msg in messages:
        send_peer_message(msg)
        time.sleep(0.5)  # Short delay between messages

    print("\nWaiting for the follower's queue to process messages (at least 5 seconds)...")
    time.sleep(5)

    print("\nQuerying follower's HTTP endpoint for user 1 objects...")
    stored = get_stored_objects(1)
    if stored is not None:
        print("Retrieved stored objects:")
        for obj in stored:
            print(obj)
    else:
        print("Failed to retrieve stored objects.")

if __name__ == "__main__":
    main()
