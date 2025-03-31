#!/usr/bin/env python3
"""
Crash test script for fault tolerance that captures follower messages.

This script uses tcpdump to capture all TCP traffic on the follower ports,
then kills the leader process (by detecting the process listening on the given leader port)
so that followers trigger an election. After a waiting period, the script prints
the captured messages exchanged between followers.

Usage:
    sudo python3 crash_test.py --port 9000

Make sure the psutil library is installed:
    pip install psutil
"""

import psutil
import argparse
import time
import sys
import subprocess
import os

def find_process_by_port(port):
    """
    Scans processes for a listening socket on the specified port.
    Returns the process if found.
    """
    print(f"[Crash Test] Searching for process listening on port {port}...")
    for proc in psutil.process_iter(attrs=['pid', 'name']):
        try:
            connections = proc.connections(kind='inet')
            for conn in connections:
                if conn.status == psutil.CONN_LISTEN and conn.laddr.port == port:
                    print(f"[Crash Test] Found process {proc.pid} ({proc.name()}) listening on port {port}.")
                    return proc
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    print(f"[Crash Test] No process found on port {port}.")
    return None

def start_tcpdump(follower_ports):
    """
    Starts tcpdump to capture TCP traffic on the provided follower ports.
    The '-A' flag prints packet contents in ASCII.
    """
    filters = " or ".join([f"port {port}" for port in [9001, 9002, 9003]])
    filter_str = f"tcp and ({filters})"

    cmd = ["tcpdump", "-l", "-i", "lo", "-A", filter_str]
    print(f"[Crash Test] Starting tcpdump with command: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc

def main():
    parser = argparse.ArgumentParser(
        description='Crash test for leader fault tolerance and capture follower messages.')
    parser.add_argument('--port', type=int, default=9000,
                        help='Peer port of the leader to crash (default: 9000)')
    parser.add_argument('--follower-ports', type=str, default="9001,9002",
                        help='Comma-separated list of follower peer ports to monitor (default: "9001,9002")')
    args = parser.parse_args()

    # Parse follower ports.
    follower_ports = [port.strip() for port in args.follower_ports.split(",") if port.strip().isdigit()]
    follower_ports = list(map(int, follower_ports))
    if not follower_ports:
        print("[Crash Test] No valid follower ports provided.")
        sys.exit(1)

    # Warn if not running as root (tcpdump may require elevated privileges)
    if os.geteuid() != 0:
        print("[Crash Test] Warning: tcpdump may require sudo privileges to capture packets on interface 'lo'.")

    # Start tcpdump to capture messages on the follower ports.
    print("[Crash Test] Launching tcpdump to capture follower messages...")
    tcpdump_proc = start_tcpdump(follower_ports)
    time.sleep(2)  # Give tcpdump a moment to start
    print("[Crash Test] tcpdump is running.")

    # Locate and crash the leader process.
    leader_proc = find_process_by_port(args.port)
    if leader_proc is None:
        print(f"[Crash Test] No process found listening on leader port {args.port}. Ensure the leader is running.")
        tcpdump_proc.terminate()
        sys.exit(1)
    
    print(f"[Crash Test] Crashing leader process: PID {leader_proc.pid}, Name: {leader_proc.name()}.")
    leader_proc.kill()
    print("[Crash Test] Leader process has been killed.")

    # Wait for fault tolerance mechanism (election) to complete.
    print("[Crash Test] Waiting for followers to detect the leader failure and trigger an election (15 seconds)...")
    time.sleep(15)

    # Stop tcpdump and capture its output.
    print("[Crash Test] Terminating tcpdump and collecting captured messages...")
    tcpdump_proc.terminate()
    # try:
    #     stdout, stderr = tcpdump_proc.communicate(timeout=5)
    # except subprocess.TimeoutExpired:
    #     tcpdump_proc.kill()
    #     stdout, stderr = tcpdump_proc.communicate()

    # print("\n=== Captured Follower Messages ===\n")
    # print(stdout)
    # print("\n=== End of Captured Messages ===\n")
    print("[Crash Test] Crash test complete.")

if __name__ == "__main__":
    main()
