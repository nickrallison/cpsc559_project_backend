#!/usr/bin/env python3

import json
import signal
import subprocess
import sys
import time


def load_config(config_file="machines.json") -> dict:
    with open(config_file, "r") as f:
        config = json.load(f)
    return config

def setup_command(machine_config, repo_url) -> str:
    repo_path = machine_config.get("repo_path", "~/.config/cpsc559")
    cmd: str = f"mkdir -p {repo_path} && cd {repo_path} && rm -f *.db; cd {repo_path} && if [ ! -d .git ]; then git clone {repo_url} .; fi && git pull"
    return cmd

def backend_command(machines: list[dict], id: int) -> str:
    num_machines = len(machines)
    assert 0 <= id < num_machines, f"Invalid machine ID: {id}. Must be between 0 and {num_machines - 1}."

    # Get the machine configuration for the given ID.
    machine = machines[id]

    # Get Other Relevant Peers
    peers = [machine for i, machine in enumerate(machines) if i != id]
    leader = [machine for machine in machines if machine["role"] == "leader"]
    assert len(leader) == 1, "There should be exactly one leader in the cluster."

    # Get all relevant info to build the run command.
    peers_list = ",".join([f"{peer['ip']}:{peer['backend_port']}" for peer in peers])
    db = machine["database"]
    role = machine["role"]
    leader_addr = f"{leader[0]['ip']}:{leader[0]['backend_port']}"

    repo_path = machine.get("repo_path", "~/.config/cpsc559")
    go_path = machine.get("go_path", "go")
    run_command = (
        f"cd {repo_path} && {go_path} run {repo_path}/src/main.go "
        f"--db {db} --role {role} --localAddr {machine['ip']} "
        f"--peerPort {machine['backend_port']} --leaderAddr {leader_addr} "
        f"--peers {peers_list} --httpHost {machine['ip']} --httpPort {machine['http_port']}"
    )
    return run_command

def frontend_command(machine_config: dict) -> str:
    repo_path = machine_config.get("repo_path", "~/.config/cpsc559")
    full_cmd = (
        f"cd {repo_path}/frontend && export REACT_APP_HTTPPORT={machine_config['ip']}:{machine_config['http_port']} && "
        f"npm start"
    )
    return full_cmd

def run_remote_command(machine_config, remote_command) -> subprocess.Popen:
    username = machine_config["username"]
    ip = machine_config["ip"]
    ssh_target = f"{username}@{ip}"

    cmd = [
        "tailscale", "ssh",
        ssh_target,
        remote_command
    ]
    cmd_str = f"tailscale ssh {ssh_target} \"{remote_command}\""
    print(f"Running command:\n{cmd_str}")

    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def main():
    # Load configuration from the JSON file.
    # The file is expected to have keys "machine1", "machine2", and "repo_url".
    config = load_config()
    machines = config["machines"]

    # Get Relevant Machines
    leader = [machine_id for (machine_id, machine) in enumerate(machines) if machine["role"] == "leader"]
    assert len(leader) == 1, "There should be exactly one leader in the cluster."
    leader_id = leader[0]
    follower_ids = [machine_id for (machine_id, machine) in enumerate(machines) if machine["role"] != "leader"]
    leader_machine = machines[leader_id]

    # Get the repo URL from the config
    repo_url = config["repo_url"]

    for machine_id, machine in enumerate(machines):
        setup_cmd = setup_command(machine, repo_url)
        print(f"Setting up machine {machine_id} with command: {setup_cmd}")
        run_remote_command(machine, setup_cmd).communicate()


    # get run commands
    leader_run_cmd = backend_command(machines, leader_id)
    follower_run_cmds = [
        (follower_id, backend_command(machines, follower_id)) for follower_id in follower_ids
    ]

    procs: list[subprocess.Popen] = []
    print(f"Starting leader instance on machine{leader_id} over SSH…")
    proc = run_remote_command(leader_machine, leader_run_cmd)
    procs.append(proc)

    for follower_id, follower_full_cmd in follower_run_cmds:
        print(f"Starting follower instance on machine{follower_id} over SSH…")
        procs.append(run_remote_command(machines[follower_id], follower_full_cmd))

    print("Cluster node 1 started:")
    print(f"  Leader HTTP endpoint: http://{leader_machine['ip']}:{leader_machine['http_port']}")
    for follower_id in follower_ids:
        machine = machines[follower_id]
        print(f"  Follower HTTP endpoint: http://{machine['ip']}:{machine['http_port']}")

    # Start the frontend on the leader machine.
    print(f"Starting frontend on leader machine: http://{leader_machine['ip']}:{leader_machine['frontend_port']}")
    leader_frontend_cmd = frontend_command(leader_machine)
    leader_frontend_proc = run_remote_command(leader_machine, leader_frontend_cmd)
    procs.append(leader_frontend_proc)

    for follower_id in follower_ids:
        follower_machine = machines[follower_id]
        print(f"Starting frontend on follower machine: http://{follower_machine['ip']}:{follower_machine['frontend_port']}")
        follower_frontend_cmd = frontend_command(machines[follower_id])
        procs.append(run_remote_command(machines[follower_id], follower_frontend_cmd))

    print("Frontends started")


    # If this script is terminated (Ctrl+C, SIGTERM, etc.) then kill the SSH sessions.
    def cleanup(sig, frame):
        print("\nTerminating remote leader and follower sessions…")
        for proc in procs:
            proc.terminate()
        # Wait for the processes to exit
        for proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    # Keep the script running.
    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()