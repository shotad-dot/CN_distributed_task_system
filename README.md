# Distributed Task Notification and Worker System

A Python-based distributed task-processing system that enables a central task manager to communicate with remote worker nodes over TCP sockets. The project uses Mininet to simulate a network of distributed hosts and Flask to provide a web-based interface for submitting tasks and viewing results.

## Overview

The system demonstrates how tasks can be submitted through a central manager, transmitted over a network to worker nodes, processed remotely, and returned to the client.

The current implementation focuses on establishing network connectivity, TCP-based communication, remote task execution, and a web-based dashboard.

## Architecture

```text
             User
              |
              v
       Web Dashboard
          (Flask)
              |
              | HTTP
              v
       Task Manager (h1)
              |
              | TCP Socket
              v
        Worker Node (h2)
              |
              | Executes Task
              v
        Computed Result
              |
              v
       Task Manager (h1)
              |
              v
        Web Dashboard
```

## Features

- **Network Simulation:** Uses Mininet to create a virtual network topology.
- **TCP Communication:** Establishes communication between the task manager and worker nodes using TCP sockets.
- **Centralized Task Manager:** Receives tasks from the web interface and forwards them to workers.
- **Remote Task Execution:** Worker nodes independently execute assigned computational tasks.
- **Web Dashboard:** Provides an interactive Flask-based interface for submitting tasks.
- **Result Delivery:** Returns computed results from workers to the dashboard.
- **Multiple Operations:** Supports arithmetic and mathematical operations.

### Supported Operations

| Operation | Description |
|---|---|
| ADD | Addition |
| SUBTRACT | Subtraction |
| MULTIPLY | Multiplication |
| DIVIDE | Division |
| MODULO | Modulo |
| POWER | Exponentiation |
| FLOOR_DIVIDE | Floor division |
| MAX | Maximum value |
| MIN | Minimum value |
| GCD | Greatest common divisor |
| LCM | Least common multiple |

## Technologies Used

- **Python** – Core application logic
- **TCP Sockets** – Network communication
- **Mininet** – Network emulation
- **Open vSwitch** – Virtual network switching
- **Flask** – Backend web framework
- **HTML, CSS and JavaScript** – Dashboard interface

## Network Topology

The project uses a Mininet topology consisting of:

| Node | IP Address | Role |
|---|---|---|
| h1 | 10.0.0.1 | Task Manager |
| h2 | 10.0.0.2 | Worker 1 |
| h3 | 10.0.0.3 | Worker 2 |
| h4 | 10.0.0.4 | Worker 3 |
| s1 | — | Open vSwitch |

The initial implementation uses one active worker (h2) for task execution. The remaining worker nodes are part of the network topology.

## Project Structure

```text
distributed-task-notification-system/
│
├── src/
│   ├── manager.py
│   └── worker.py
│
├── topology/
│   └── topology.py
│
├── templates/
│   └── index.html
│
├── static/
│
├── logs/
│
├── results/
│
└── README.md
```

## Setup and Execution

### Prerequisites

- Linux (Ubuntu/Xubuntu recommended)
- Python 3
- Mininet
- Open vSwitch
- Flask

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/distributed-task-notification-system.git

cd distributed-task-notification-system
```

### 2. Set Up the Python Environment

```bash
python3 -m venv venv

source venv/bin/activate

pip install flask
```

### 3. Start the Mininet Topology

```bash
sudo mn -c

sudo mn --custom topology/topology.py --topo worker --controller none
```

Inside the Mininet CLI:

```bash
sh ovs-vsctl set-fail-mode s1 standalone

pingall
```

### 4. Start the Worker Node

Open a terminal for h2:

```bash
xterm h2
```

Inside the h2 terminal:

```bash
cd ~/distributed-task-system

source venv/bin/activate

python src/worker.py
```

### 5. Start the Task Manager

Open another terminal for h1:

```bash
xterm h1
```

Inside the h1 terminal:

```bash
cd ~/distributed-task-system

source venv/bin/activate

python src/manager.py
```

### 6. Access the Dashboard

Open the dashboard in your desktop browser:

```text
http://10.0.0.1:8000
```

Submit a task using the operation selector and input fields. The manager forwards the task to the worker, which processes it and returns the result.

> Note: Desktop-to-Mininet connectivity may require an additional virtual interface configuration, depending on the host environment.

## Future Enhancements

The next development phase aims to introduce more advanced distributed-system capabilities:

- Dynamic worker registration and management
- Multi-worker task distribution
- Workload-aware task scheduling
- Worker workload monitoring
- Task timeout management
- Worker failure detection
- Automatic task redistribution
- SDN-based network management and traffic control
- Scheduling efficiency evaluation

## Project Objective

The primary objective is to understand and implement the fundamentals of distributed task processing, network communication, and worker-based computation, while progressively exploring intelligent scheduling and Software-Defined Networking (SDN).

---

**Developed as an academic Computer Networks project.**
