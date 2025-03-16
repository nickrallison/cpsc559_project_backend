#!/usr/bin/env python3
# This only tests order in which the updates are applied only on one single node. It sends the request directly to the peer port of the "follower"
# since the metadata points to the leader's address, the peer assumes that it is already coming in from the leader, therefore, this script won't trigger any actions on the leader side/
# make sure only "follower" is running on port 8081
import random
import string
import time

import requests


class StoredObject:
    def __init__(self, user_id, user_message_id, data):
        self.user_id = user_id
        self.user_message_id = user_message_id
        self.data = data

    def as_dict(self):
        return {
            "user_id": self.user_id,
            "user_message_id": self.user_message_id,
            "data": self.data
        }

def get_objects_for_user(user_id):
    url = f'http://localhost:8080/objects?userId={user_id}'
    response = requests.get(url)
    if response.status_code != 200:
        print(f"GET for user {user_id} failed with {response.status_code}. Response: {response.text}")
        return []
    try:
        return response.json()
    except Exception as e:
        print(f"Error decoding response JSON for user {user_id}: {e}")
        return []

def delete_object(user_id, user_message_id):
    url = f'http://localhost:8080/objects?userId={user_id}&userMessageId={user_message_id}'
    response = requests.delete(url)
    return response

def main():
    print("=== Step 1: GET all objects for users 1, 2, and 3 ===")
    existing_objects = {}
    for user_id in [1, 2, 3]:
        objs = get_objects_for_user(user_id)
        existing_objects[user_id] = objs
        print(f"User {user_id} existing objects:", objs)

    print("\n=== Step 2: DELETE all objects for users 1, 2, and 3 ===")
    for user_id, objs in existing_objects.items():
        for obj in objs:
            msg_id = obj.get("user_message_id")
            resp = delete_object(user_id, msg_id)
            if resp.status_code == 200:
                print(f"Deleted object for user {user_id}, message {msg_id}")
            else:
                print(f"Failed to delete object for user {user_id}, message {msg_id}, status {resp.status_code}: {resp.text}")

    # Verify deletion; the GET should now return empty lists.
    print("\nVerifying deletion:")
    for user_id in [1, 2, 3]:
        objs = get_objects_for_user(user_id)
        if objs:
            print(f"Warning: User {user_id} still has objects:", objs)
        else:
            print(f"User {user_id} has no objects.")

    print("\n=== Step 3: Create 5 messages per user (users 1,2,3) ===")
    new_objects = []
    expected_objects = {1: [], 2: [], 3: []}
    for user_id in [1, 2, 3]:
        # We'll use a set to ensure distinct user_message_id values per user.
        used_ids = set()
        for i in range(5):
            # Generate a distinct random 32-bit unsigned integer
            msg_id = random.getrandbits(32)
            while msg_id in used_ids:
                msg_id = random.getrandbits(32)
            used_ids.add(msg_id)
            # Generate a random payload of 8 characters (a-z, A-Z, 0-9)
            payload = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
            so = StoredObject(user_id, msg_id, payload)
            new_objects.append(so)
            expected_objects[user_id].append(so.as_dict())
            print(f"Created for user {user_id}: {so.as_dict()}")

    print("\n=== Step 4: POST the new messages to the DB ===")
    post_url = 'http://localhost:8080/objects'
    post_payload = [obj.as_dict() for obj in new_objects]
    resp = requests.post(post_url, json=post_payload)
    if resp.status_code != 200:
        print("POST failed with status", resp.status_code, resp.text)
        return
    else:
        print("POST succeeded, response:", resp.json())

    # Allow a short delay to let the server process (and replicate if needed)
    time.sleep(0.2)

    print("\n=== Step 5: GET all objects for users 1,2,3 after POST ===")
    retrieved_objects = {}
    for user_id in [1, 2, 3]:
        objs = get_objects_for_user(user_id)
        retrieved_objects[user_id] = objs
        print(f"User {user_id} retrieved objects:", objs)

    print("\n=== Step 6: Assert that retrieved objects match expected objects ===")
    success = True
    # For each user, sort by user_message_id (or any stable order) so order isn’t an issue.
    for user_id in [1, 2, 3]:
        expected_sorted = sorted(expected_objects[user_id], key=lambda x: x["user_message_id"])
        retrieved_sorted = sorted(retrieved_objects[user_id], key=lambda x: x["user_message_id"])
        if expected_sorted != retrieved_sorted:
            print(f"Mismatch for user {user_id}:")
            print("Expected:", expected_sorted)
            print("Retrieved:", retrieved_sorted)
            success = False
        else:
            print(f"User {user_id}: retrieved objects match expected.")
    if success:
        print("\nAll objects match the expected values.")
    else:
        print("\nThere were mismatches between the expected and retrieved objects.")

if __name__ == '__main__':
    main()