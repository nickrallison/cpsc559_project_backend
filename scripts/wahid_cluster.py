#!/usr/bin/env python3
"""
Cluster Node 2:
Starts two follower instances on machine2.
Replace <machine2_ip> with the Tailscale IP (or MagicDNS hostname) of machine2,
and <machine1_ip> with the Tailscale IP (or MagicDNS hostname) of the leader machine.
"""

import subprocess
import signal
import sys
import time
import os

def main():
    # Replace these with your actual Tailscale addresses.
    machine2_ip = "100.103.172.113"
    machine1_ip = "100.81.146.3"

    # The full peers list (must match the one used by all nodes):
    peers_list = f"{machine1_ip}:9001,{machine2_ip}:9002,{machine2_ip}:9003"

    # Follower instance 1 on machine2
    follower1_cmd = [
        "go", "run", "./src/main.go",
        "--db", "file:follower2.db",
        "--role", "follower",
        "--localAddr", machine2_ip,
        "--peerPort", "9002",
        "--leaderAddr", f"{machine1_ip}:9000",
        "--peers", peers_list,
        "--httpHost", machine2_ip,
        "--httpPort", "8082"
    ]
    follower1_env = os.environ.copy()
    follower1_env["ENV_FILE"] = ".env.follower"

    # Follower instance 2 on machine2
    follower2_cmd = [
        "go", "run", "./src/main.go",
        "--db", "file:follower3.db",
        "--role", "follower",
        "--localAddr", machine2_ip,
        "--peerPort", "9003",
        "--leaderAddr", f"{machine1_ip}:9000",
        "--peers", peers_list,
        "--httpHost", machine2_ip,
        "--httpPort", "8083"
    ]
    follower2_env = os.environ.copy()
    follower2_env["ENV_FILE"] = ".env.follower"

    print("Starting follower 1 instance on machine2...")
    follower1_proc = subprocess.Popen(follower1_cmd, env=follower1_env)
    print("Starting follower 2 instance on machine2...")
    follower2_proc = subprocess.Popen(follower2_cmd, env=follower2_env)

    print("Cluster node 2 started:")
    print(f"  Follower 1 HTTP endpoint: http://{machine2_ip}:8082")
    print(f"  Follower 2 HTTP endpoint: http://{machine2_ip}:8083")

    def cleanup(sig, frame):
        print("\nTerminating followers on machine2...")
        follower1_proc.terminate()
        follower2_proc.terminate()
        follower1_proc.wait()
        follower2_proc.wait()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    
    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()
