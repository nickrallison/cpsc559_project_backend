#!/usr/bin/env python3
# quickly starup 1 leader and 2 follower instances - note DB will be cleared
import subprocess
import os
import time
import signal

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

LEADER_DB = "leader.db"
FOLLOWER1_DB = "follower1.db"
FOLLOWER2_DB = "follower2.db"

# Hard-coded flags for leader and followers
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

def remove_db_files():
    """Remove old DB files."""
    for dbfile in [LEADER_DB, FOLLOWER1_DB, FOLLOWER2_DB]:
        dbpath = os.path.join(ROOT_DIR, dbfile)
        if os.path.exists(dbpath):
            os.remove(dbpath)
            print(f"Removed old DB: {dbfile}")

def start_process(command_args):
    """Start a Go process with given command arguments."""
    return subprocess.Popen(command_args, cwd=ROOT_DIR)

def main():
    remove_db_files()

    print("Starting leader and follower processes...")
    
    # Start processes
    leader_proc = start_process(LEADER_FLAGS)
    follower1_proc = start_process(FOLLOWER1_FLAGS)
    follower2_proc = start_process(FOLLOWER2_FLAGS)

    processes = [leader_proc, follower1_proc, follower2_proc]

    try:
        print("\nCluster started successfully!")
        print("- Leader: http://localhost:8080")
        print("- Follower1: http://localhost:8081")
        print("- Follower2: http://localhost:8082")
        print("\nPress Ctrl+C to stop the cluster...")

        # Keep the script running until interrupted
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down processes...")
        for proc in processes:
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        print("All processes terminated successfully!")

if __name__ == "__main__":
    main()
