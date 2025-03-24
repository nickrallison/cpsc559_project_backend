#!/usr/bin/env python3
"""
Start a cluster of one leader and three followers.

Each instance is started by invoking the Go main program with different arguments.
The leader listens on peer port 9000 and HTTP port 8080.
Follower 1 listens on peer port 9001 and HTTP port 8081.
Follower 2 listens on peer port 9002 and HTTP port 8082.
Follower 3 listens on peer port 9003 and HTTP port 8083.

Usage:
    python3 start_cluster.py
"""

import subprocess
import signal
import sys
import time
import os

def main():
    # Leader command: set ENV_FILE to .env.leader in the environment.
    leader_cmd = [
        "go", "run", "./src/main.go",
        "--db", "file:leader.db",
        "--role", "leader",
        "--localAddr", "localhost",
        "--peerPort", "9000",
        "--peers", "localhost:9001,localhost:9002,localhost:9003",
        "--httpHost", "localhost",
        "--httpPort", "8080"
    ]
    leader_env = os.environ.copy()
    leader_env["ENV_FILE"] = ".env.leader"

    # Follower commands: pass the peers list to each follower.
    follower_cmds = [
        (
            [
                "go", "run", "./src/main.go",
                "--db", "file:follower1.db",
                "--role", "follower",
                "--localAddr", "localhost",
                "--peerPort", "9001",
                "--leaderAddr", "localhost:9000",
                "--peers", "localhost:9001,localhost:9002,localhost:9003",
                "--httpHost", "localhost",
                "--httpPort", "8081"
            ],
            ".env.follower"
        ),
        (
            [
                "go", "run", "./src/main.go",
                "--db", "file:follower2.db",
                "--role", "follower",
                "--localAddr", "localhost",
                "--peerPort", "9002",
                "--leaderAddr", "localhost:9000",
                "--peers", "localhost:9001,localhost:9002,localhost:9003",
                "--httpHost", "localhost",
                "--httpPort", "8082"
            ],
            ".env.follower"
        ),
        (
            [
                "go", "run", "./src/main.go",
                "--db", "file:follower3.db",
                "--role", "follower",
                "--localAddr", "localhost",
                "--peerPort", "9003",
                "--leaderAddr", "localhost:9000",
                "--peers", "localhost:9001,localhost:9002,localhost:9003",
                "--httpHost", "localhost",
                "--httpPort", "8083"
            ],
            ".env.follower"
        )
    ]

    print("Starting leader instance...")
    leader_proc = subprocess.Popen(leader_cmd, env=leader_env)
    follower_procs = []
    
    for i, (cmd, env_file) in enumerate(follower_cmds):
        print(f"Starting follower {i+1} instance...")
        env = os.environ.copy()
        env["ENV_FILE"] = env_file
        proc = subprocess.Popen(cmd, env=env)
        follower_procs.append(proc)
    
    print("All instances started successfully.")
    print("Leader HTTP endpoint: http://localhost:8080")
    print("Follower endpoints: http://localhost:8081, http://localhost:8082, http://localhost:8083")
    
    # Define a cleanup function to terminate processes on exit.
    def cleanup(sig, frame):
        print("\nTerminating all instances...")
        leader_proc.terminate()
        for p in follower_procs:
            p.terminate()
        leader_proc.wait()
        for p in follower_procs:
            p.wait()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    
    # Keep the script running indefinitely.
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup(None, None)

if __name__ == '__main__':
    main()
