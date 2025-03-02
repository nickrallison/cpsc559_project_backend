#!/usr/bin/env python3
import signal
import subprocess
import sys
import time


def main():
    #   – HTTP server will run on http://localhost:8080
    leader_cmd = [
        "go", "run", "src/main.go",
        "--db", "file:leader.db",
        "--role", "leader",
        "--peerPort", "9000",
        "--peers", "localhost:9002",
        "--httpHost", "localhost",
        "--httpPort", "8080"
    ]

    #   – HTTP server will run on http://localhost:8081
    follower_cmd = [
        "go", "run", "src/main.go",
        "--db", "file:follower.db",
        "--role", "follower",
        "--peerPort", "9002",
        "--leaderAddr", "localhost:9000",
        "--httpHost", "localhost",
        "--httpPort", "8081"
    ]

    print("Starting leader and follower instances...")

    try:
        # Start the leader process.
        leader_proc = subprocess.Popen(leader_cmd)
        # Start the follower process.
        follower_proc = subprocess.Popen(follower_cmd)
    except Exception as e:
        print(f"Error starting processes: {e}")
        sys.exit(1)

    # Give the servers some time to start up (adjust sleep time if needed).
    time.sleep(2)

    # Print the HTTP addresses for both instances.
    leader_http = "http://localhost:8080"
    follower_http = "http://localhost:8081"
    print("\nInstances started successfully:")
    print(f"Leader HTTP address: {leader_http}")
    print(f"Follower HTTP address: {follower_http}\n")

    # Define a signal handler to cleanup subprocesses on exit.
    def signal_handler(sig, frame):
        print("\nTerminating the Go instances...")
        leader_proc.terminate()
        follower_proc.terminate()
        leader_proc.wait()
        follower_proc.wait()
        sys.exit(0)

    # Register the signal handler for Ctrl+C.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Wait indefinitely while the processes run.
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(None, None)

if __name__ == "__main__":
    main()