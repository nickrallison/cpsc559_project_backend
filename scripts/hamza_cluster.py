#!/usr/bin/env python3
"""
Cluster Node 1:
Starts the leader and one follower on machine1.
Replace <machine1_ip> with the Tailscale IP (or MagicDNS hostname) of machine1,
and <machine2_ip> with the Tailscale IP (or MagicDNS hostname) of machine2.
"""

import subprocess
import signal
import sys
import time
import os

def main():
    # Replace these with your actual Tailscale addresses.
    machine1_ip = "100.81.146.3"
    machine2_ip = "100.103.172.113"

    # Define the full peers list for the cluster:
    # - Follower on machine1 (local) will use port 9001.
    # - Two followers on machine2 will use ports 9002 and 9003.
    peers_list = f"{machine1_ip}:9001,{machine2_ip}:9002,{machine2_ip}:9003"

    # Leader command
    leader_cmd = [
        "go", "run", "./src/main.go",
        "--db", "file:leader.db",
        "--role", "leader",
        "--localAddr", machine1_ip,
        "--peerPort", "9000",
        "--peers", peers_list,
        "--httpHost", machine1_ip,
        "--httpPort", "8080"
    ]
    leader_env = os.environ.copy()
    leader_env["ENV_FILE"] = ".env.leader"

    # Follower command (the one running on machine1)
    follower_cmd = [
        "go", "run", "./src/main.go",
        "--db", "file:follower1.db",
        "--role", "follower",
        "--localAddr", machine1_ip,
        "--peerPort", "9001",
        "--leaderAddr", f"{machine1_ip}:9000",
        "--peers", peers_list,
        "--httpHost", machine1_ip,
        "--httpPort", "8081"
    ]
    follower_env = os.environ.copy()
    follower_env["ENV_FILE"] = ".env.follower"

    print("Starting leader instance on machine1...")
    leader_proc = subprocess.Popen(leader_cmd, env=leader_env)

    print("Starting follower instance on machine1...")
    follower_proc = subprocess.Popen(follower_cmd, env=follower_env)

    print("Cluster node 1 started:")
    print(f"  Leader HTTP endpoint: http://{machine1_ip}:8080")
    print(f"  Follower HTTP endpoint: http://{machine1_ip}:8081")

    def cleanup(sig, frame):
        print("\nTerminating leader and follower on machine1...")
        leader_proc.terminate()
        follower_proc.terminate()
        leader_proc.wait()
        follower_proc.wait()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    
    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()
