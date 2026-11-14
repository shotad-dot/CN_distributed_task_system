# Distributed Task Notification and Worker System (DST)

A Mininet-based distributed job-processing demonstration. The browser submits a sample job to a Flask manager running in Mininet host `h1`. The manager partitions the job, schedules chunks across dynamically created Mininet worker hosts, communicates with their TCP servers, and aggregates the results.

## Implemented

- Dynamic creation and deletion of real Mininet hosts from the dashboard.
- One TCP worker process per host, listening on port 5000.
- TCP JSON-line request/response protocol between manager and workers.
- Workload-aware scheduling using outstanding work, capacity, and EWMA response time.
- Parallel chunk dispatch and result aggregation for matrix multiplication, sum, prime discovery, and sorting.
- Basic failure recovery: if a worker connection/task fails, the manager retries that subtask on other active workers before marking it failed.
- Dashboard with worker status, task traces, results, and performance counters.
- Worker startup is checked using the worker process's explicit listening message; the launcher avoids blocking `host.cmd()` readiness probes.

## Not implemented in this build

The switch is Open vSwitch with OpenFlow 1.3 support and is currently placed in **standalone learning-switch mode** so the demonstration can forward traffic without an external controller. This build does **not** include a running Ryu controller or SDN-controlled workload routing. Do not present it as a completed Ryu/SDN implementation.

The retry mechanism is a basic application-level retry: it tries other active workers when a TCP task attempt fails. It is not a production-grade fault-tolerance protocol; a worker crash after completing work but before returning its response can cause the same pure computation to be executed again.

The `src/operations.py` module is included as a scalar-arithmetic compatibility helper. The distributed sample jobs use the chunk-based `run_task()` implementation in `src/worker.py`.

## Requirements

Ubuntu/Xubuntu with Mininet and Open vSwitch installed, Python 3, and `python3-venv`.

```bash
sudo apt update
sudo apt install mininet openvswitch-switch python3-venv python3-pip
cd ~/DST-Final/dst
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

If `venv` already exists, do not recreate it unnecessarily; just install requirements with `./venv/bin/pip install -r requirements.txt`.

## Run

From the project directory:

```bash
sudo mn -c
sudo python3 scripts/run_network.py
```

Open the desktop browser at **http://10.0.0.1:8000**.

1. Click **Create worker** one or more times.
2. Wait for each worker to appear as ACTIVE.
3. Choose a sample job.
4. Expand the job trace to see subtask assignments.
5. Remove idle workers when finished.
6. Press Ctrl+C in the launcher terminal to stop the network.

## Network layout

- Manager host: `h1`, `10.0.0.1`, Flask port `8000`.
- OVS switch: `s1`, OpenFlow 1.3 protocol configured.
- Desktop OVS internal interface: `host0`, `10.0.0.254`.
- Worker management API: `10.0.0.254:8765` in the root namespace.
- Workers: `10.0.0.2`, `10.0.0.3`, etc.; TCP port `5000`.

## Architecture

`Browser → Flask manager (h1) → partitioner/scheduler → TCP worker hosts → partial results → aggregation → dashboard`

`Flask manager → root-side Mininet orchestrator → net.addHost()/net.addLink()/OVS attach`

The root-side orchestrator owns the Mininet object because a Flask process inside `h1` cannot directly mutate the Mininet object created by the launcher in the root namespace.

## Troubleshooting

- If startup reports an existing Mininet/OVS process, stop the previous launcher and run `sudo mn -c` once.
- If the dashboard does not load, check that the launcher printed `DST NETWORK STARTED` and the dashboard address.
- If worker creation fails, read the exact `FAILED creating wN` message in the launcher terminal. The API returns the underlying exception as JSON.
- This project is intended for a trusted local lab network; the management API has no authentication.

## CONTRIBUTION OF MEMBERS:

- Mininet setup,Topology,TCP Socket establishment,Dashboard-Harrshit
- Worker node creation,task distribution,EWMA Algorithm-Hannah
-	Manager setup,timeout,task redistribution-Harshita
