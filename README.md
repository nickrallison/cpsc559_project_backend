# CPSC 559 Project

## Run Instructions

### Prerequisites

- Each node must:
  - install go & yarn
  - Have correct git credentials
  - Have tailscale ssh enabled

### Instructions

Run the following command to start the server:
```sh
go run ./src
```

Run this command to run the python script to send a request to the server and receive a response:
```sh
python ./scripts/test_script.py
```

## Test

```sh
go test ./... -v
```