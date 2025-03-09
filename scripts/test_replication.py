#!/usr/bin/env python3
"""
Step-by-step test script for your leader-follower distributed system.
Ensures each run starts with fresh DB files so the logs match the actual step order.
"""

import subprocess
import time
import requests
import os
import threading

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
TEST_RESULTS_PATH = os.path.join(ROOT_DIR, "test_results.log")

LEADER_DB = "leader.db"
FOLLOWER1_DB = "follower1.db"
FOLLOWER2_DB = "follower2.db"

# Hard-coded ports/roles for each instance
LEADER_FLAGS = [
    "go", "run", "src/main.go",
    "--role=leader",
    "--peerPort=9000",
    "--peers=localhost:9001,localhost:9002",
    "--httpPort=8080",
    f"--db=file:{LEADER_DB}"
]
FOLLOWER1_FLAGS = [
    "go", "run", "src/main.go",
    "--role=follower",
    "--peerPort=9001",
    "--leaderAddr=localhost:9000",
    "--httpPort=8081",
    f"--db=file:{FOLLOWER1_DB}"
]
FOLLOWER2_FLAGS = [
    "go", "run", "src/main.go",
    "--role=follower",
    "--peerPort=9002",
    "--leaderAddr=localhost:9000",
    "--httpPort=8082",
    f"--db=file:{FOLLOWER2_DB}"
]

LEADER_HTTP_PORT = 8080
FOLLOWER1_HTTP_PORT = 8081
FOLLOWER2_HTTP_PORT = 8082

###############################################################
# HTTP Helper Methods
###############################################################
def post_objects(port, objects):
    url = f"http://localhost:{port}/objects"
    resp = requests.post(url, json=objects)
    resp.raise_for_status()
    return resp.json()

def put_object(port, obj):
    existing_obj = get_object(port, obj["user_id"], obj["user_message_id"])
    obj["sequence_number"] = existing_obj["sequence_number"] + 1
    url = f"http://localhost:{port}/objects"
    resp = requests.put(url, json=obj)
    resp.raise_for_status()
    return resp.json()

def delete_object(port, user_id, user_message_id):
    obj = get_object(port, user_id, user_message_id)
    sequence_number = obj["sequence_number"]
    url = f"http://localhost:{port}/objects?userId={user_id}&userMessageId={user_message_id}&sequenceNumber={sequence_number}"
    resp = requests.delete(url)
    resp.raise_for_status()
    return resp.json()

def get_object(port, user_id, user_message_id):
    objs = get_objects(port, user_id)
    for obj in objs:
        if obj["user_message_id"] == user_message_id:
            return obj
    raise Exception(f"Object with userId={user_id} and messageId={user_message_id} not found.")

def get_objects(port, user_id):
    url = f"http://localhost:{port}/objects?userId={user_id}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def get_all_objects(port):
    url = f"http://localhost:{port}/allObjects"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

###############################################################
# Logging
###############################################################
def log_and_print(message, fh):
    print(message)
    fh.write(message + "\n")

def log_node_states(fh, label):
    """
    Writes a snapshot of all objects from each node to the results file,
    with an optional label describing the test step.
    """
    log_and_print(f"\n=== {label} - Node States ===", fh)
    for name, port in [
        ("Leader", LEADER_HTTP_PORT),
        ("Follower1", FOLLOWER1_HTTP_PORT),
        ("Follower2", FOLLOWER2_HTTP_PORT)
    ]:
        try:
            all_objs = get_all_objects(port)
            log_and_print(f"{name} (port {port}) objects: {all_objs}", fh)
        except Exception as e:
            log_and_print(f"{name} (port {port}) error retrieving /allObjects: {e}", fh)

def check_sequence_order(objs):
    seq_numbers = [obj["sequence_number"] for obj in objs]
    return seq_numbers == sorted(seq_numbers)

###############################################################
# Process Management
###############################################################
def run_go_instance(command_args):
    return subprocess.Popen(command_args, cwd=ROOT_DIR)

def cleanup(processes):
    for proc in processes:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

def wait_for_startup(seconds=5):
    print(f"Waiting {seconds}s for servers to start...")
    time.sleep(seconds)

###############################################################
# Test Steps
###############################################################
def test_insert_multiple(fh):
    log_and_print("\n==> Test: Insert multiple objects (userId=1)", fh)
    to_insert = [
        {"user_id": 1, "user_message_id": 101, "data": "Obj101"},
        {"user_id": 1, "user_message_id": 102, "data": "Obj102"},
    ]
    resp = post_objects(LEADER_HTTP_PORT, to_insert)
    log_and_print(f"Insert response: {resp}", fh)

    # Check
    leader_objs = get_objects(LEADER_HTTP_PORT, 1)
    f1_objs = get_objects(FOLLOWER1_HTTP_PORT, 1)
    f2_objs = get_objects(FOLLOWER2_HTTP_PORT, 1)

    if len(leader_objs) >= 2 and len(f1_objs) >= 2 and len(f2_objs) >= 2:
        log_and_print("SUCCESS: Inserted objects replicated to all nodes!", fh)
    else:
        log_and_print("FAILURE: Some node did not receive the inserted objects.", fh)

    # Snapshot DB states
    log_node_states(fh, "After test_insert_multiple")

def test_update_one(fh):
    log_and_print("\n==> Test: Update object userId=1, messageId=101", fh)
    updated_obj = {
        "user_id": 1,
        "user_message_id": 101,
        "data": "Obj101-Updated"
    }
    resp = put_object(LEADER_HTTP_PORT, updated_obj)
    log_and_print(f"Update response: {resp}", fh)

    # Check
    leader_objs = get_objects(LEADER_HTTP_PORT, 1)
    f1_objs = get_objects(FOLLOWER1_HTTP_PORT, 1)
    f2_objs = get_objects(FOLLOWER2_HTTP_PORT, 1)

    check_leader = next((o for o in leader_objs if o["user_message_id"] == 101), None)
    check_f1 = next((o for o in f1_objs if o["user_message_id"] == 101), None)
    check_f2 = next((o for o in f2_objs if o["user_message_id"] == 101), None)

    if (check_leader and check_f1 and check_f2 and
        check_leader["data"] == "Obj101-Updated" and
        check_f1["data"] == "Obj101-Updated" and
        check_f2["data"] == "Obj101-Updated"):
        log_and_print("SUCCESS: Update replicated!", fh)
    else:
        log_and_print("FAILURE: Update not visible on all nodes.", fh)

    # Snapshot
    log_node_states(fh, "After test_update_one")

def test_delete(fh):
    log_and_print("\n==> Test: Delete object userId=1, messageId=102", fh)
    resp = delete_object(LEADER_HTTP_PORT, 1, 102)
    log_and_print(f"Delete response: {resp}", fh)
    time.sleep(1)  # let replication settle

    leader_objs = get_objects(LEADER_HTTP_PORT, 1)
    f1_objs = get_objects(FOLLOWER1_HTTP_PORT, 1)
    f2_objs = get_objects(FOLLOWER2_HTTP_PORT, 1)

    still_leader = any(o["user_message_id"] == 102 for o in leader_objs)
    still_f1 = any(o["user_message_id"] == 102 for o in f1_objs)
    still_f2 = any(o["user_message_id"] == 102 for o in f2_objs)

    if not still_leader and not still_f1 and not still_f2:
        log_and_print("SUCCESS: Delete replicated to all nodes!", fh)
    else:
        log_and_print("FAILURE: Some node still has the deleted object!", fh)

    log_node_states(fh, "After test_delete")

def concurrency_thread_func(thread_id, n_inserts):
    for i in range(n_inserts):
        msg_id = thread_id * 1000 + i
        obj = [{"user_id": 3, "user_message_id": msg_id, "data": f"Thread{thread_id}-Msg{msg_id}"}]
        post_objects(LEADER_HTTP_PORT, obj)

def test_concurrency(fh):
    log_and_print("\n==> Test: Concurrency with multiple threads inserting userId=3", fh)

    threads = []
    num_threads = 3
    inserts_per_thread = 5
    for t_id in range(num_threads):
        t = threading.Thread(target=concurrency_thread_func, args=(t_id, inserts_per_thread))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # Wait a bit for replication
    time.sleep(2)
    expected_count = num_threads * inserts_per_thread

    l_objs = get_objects(LEADER_HTTP_PORT, 3)
    f1_objs = get_objects(FOLLOWER1_HTTP_PORT, 3)
    f2_objs = get_objects(FOLLOWER2_HTTP_PORT, 3)

    msg = (f"Leader sees {len(l_objs)} objects, "
           f"Follower1 sees {len(f1_objs)}, "
           f"Follower2 sees {len(f2_objs)} for userId=3")
    log_and_print(msg, fh)

    if len(l_objs) >= expected_count and len(f1_objs) >= expected_count and len(f2_objs) >= expected_count:
        log_and_print("SUCCESS: Concurrency test data on all nodes!", fh)
    else:
        log_and_print("FAILURE: Some node is missing data from concurrency test!", fh)

    log_node_states(fh, "After test_concurrency")

def test_sequence_replication(fh):
    objs = get_all_objects(LEADER_HTTP_PORT)
    if check_sequence_order(objs):
        log_and_print("SUCCESS: Leader objects are in correct sequence order.", fh)
    else:
        log_and_print("FAILURE: Leader objects are NOT in correct sequence order.", fh)
    log_node_states(fh, "After test_sequence_replication")

###############################################################
# Main
###############################################################
def main():
    # 1. Remove old DB files so each run is fresh
    for dbfile in [LEADER_DB, FOLLOWER1_DB, FOLLOWER2_DB]:
        dbpath = os.path.join(ROOT_DIR, dbfile)
        if os.path.exists(dbpath):
            os.remove(dbpath)

    # 2. Open test_results.log
    with open(TEST_RESULTS_PATH, "w", encoding="utf-8") as fh:
        processes = []
        try:
            # Start processes
            leader_proc = run_go_instance(LEADER_FLAGS)
            f1_proc = run_go_instance(FOLLOWER1_FLAGS)
            f2_proc = run_go_instance(FOLLOWER2_FLAGS)
            processes = [leader_proc, f1_proc, f2_proc]

            wait_for_startup(5)

            # 3. Run tests in sequence
            test_insert_multiple(fh)
            test_update_one(fh)
            test_delete(fh)
            test_concurrency(fh)
            test_sequence_replication(fh)

            log_and_print("\n=== All tests completed successfully! ===", fh)

        except Exception as e:
            log_and_print(f"Exception during tests: {e}", fh)
        finally:
            cleanup(processes)
            log_and_print("Cleaned up processes.", fh)

if __name__ == "__main__":
    main()
