#!/usr/bin/env python3
"""Own the Mininet network and expose a small worker lifecycle API."""
import json
import os
import select
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from mininet.link import TCLink
from mininet.log import info, setLogLevel
from mininet.net import Mininet
from mininet.node import OVSSwitch

ROOT = Path(__file__).resolve().parents[1]
VENV_PYTHON = ROOT / "venv" / "bin" / "python"
PYTHON = str(VENV_PYTHON if VENV_PYTHON.exists() else Path(sys.executable))
MANAGER_IP = "10.0.0.1"
DESKTOP_IP = "10.0.0.254"
AGENT_PORT = 8765
WORKER_PORT = 5000
DASHBOARD_PORT = 8000

net = None
switch = None
manager_proc = None
active = {}
next_id = 1
lock = threading.RLock()


def configure_desktop_interface():
    """Attach an OVS internal port so the desktop browser can reach h1."""
    subprocess.run([
        "ovs-vsctl", "--may-exist", "add-port", "s1", "host0", "--",
        "set", "interface", "host0", "type=internal"
    ], check=True)
    subprocess.run(["ip", "addr", "replace", f"{DESKTOP_IP}/24", "dev", "host0"], check=True)
    subprocess.run(["ip", "link", "set", "host0", "up"], check=True)


def wait_for_worker_start(proc, timeout=15):
    """Wait for the worker's explicit 'listening' message without host.cmd()."""
    deadline = time.monotonic() + timeout
    output = ""
    if proc.stdout is None:
        raise RuntimeError("Worker stdout pipe was not created")
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            try:
                output += proc.stdout.read().decode(errors="replace")
            except Exception:
                pass
            raise RuntimeError("Worker process exited during startup. " + output.strip())
        ready, _, _ = select.select([proc.stdout], [], [], 0.2)
        if ready:
            line = proc.stdout.readline()
            if not line:
                continue
            decoded = line.decode(errors="replace") if isinstance(line, bytes) else line
            output += decoded
            if "Worker listening on" in decoded:
                return
    raise RuntimeError(
        f"Worker did not announce that it was listening within {timeout}s. "
        f"Process alive={proc.poll() is None}. Output: {output.strip()}"
    )


def stop_process(proc):
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass


def remove_worker_locked(wid):
    worker = active.get(wid)
    if worker is None:
        return False
    stop_process(worker.get("proc"))
    try:
        # delHost also removes links associated with the host.
        net.delHost(worker["host"])
    except Exception:
        try:
            worker["host"].stop()
        except Exception:
            pass
        raise
    del active[wid]
    return True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        info("*** Worker API: " + (fmt % args) + "\n")

    def reply(self, code, obj):
        raw = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(raw)
        except BrokenPipeError:
            # Client timeout/disconnect; the original operation result is logged.
            pass

    def do_POST(self):
        global next_id
        if self.path.startswith("/workers/") and self.path.endswith("/fail"):
            wid = self.path.split("/")[2]
            with lock:
                worker = active.get(wid)
                if worker is None:
                    return self.reply(404, {"error": "Worker not found"})
                stop_process(worker.get("proc"))
                worker["status"] = "FAILED"
                info(f"*** FAILURE INJECTED: stopped {wid}\n")
                public = {k: v for k, v in worker.items() if k not in ("host", "proc")}
                return self.reply(200, {"worker": public, "message": f"{wid} stopped and marked FAILED"})

        if self.path != "/workers":
            return self.reply(404, {"error": "Endpoint not found"})

        with lock:
            if next_id > 253:
                return self.reply(507, {"error": "Worker IP address pool exhausted"})
            wid = f"w{next_id}"
            worker_number = next_id
            ip = f"10.0.0.{next_id + 1}"
            next_id += 1
            host = None
            proc = None
            try:
                info(f"*** Creating {wid} at {ip}\n")
                host = net.addHost(wid, ip=f"{ip}/24")
                link = net.addLink(host, switch, cls=TCLink)
                host.configDefault()
                switch.attach(link.intf2)
                proc = host.popen(
                    [PYTHON, "-u", "-m", "src.worker", str(WORKER_PORT)],
                    cwd=str(ROOT), stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    env={**os.environ, "PYTHONUNBUFFERED": "1"},
                )
                wait_for_worker_start(proc)
                worker = {
                    "id": wid, "name": f"Worker {worker_number}", "ip": ip,
                    "status": "ACTIVE", "outstanding": 0, "capacity": 1,
                    "ewma_seconds": 0.05, "completed": 0, "failed": 0,
                    "host": host, "proc": proc,
                }
                active[wid] = worker
                info(f"*** {wid} ready: {ip}:{WORKER_PORT}\n")
                public = {k: v for k, v in worker.items() if k not in ("host", "proc")}
                return self.reply(201, public)
            except Exception as exc:
                info(f"*** FAILED creating {wid}: {type(exc).__name__}: {exc}\n")
                stop_process(proc)
                if host is not None:
                    try:
                        net.delHost(host)
                    except Exception as cleanup_exc:
                        info(f"*** Cleanup warning for {wid}: {cleanup_exc}\n")
                return self.reply(500, {"error": f"{type(exc).__name__}: {exc}"})

    def do_DELETE(self):
        wid = self.path.rsplit("/", 1)[-1]
        with lock:
            worker = active.get(wid)
            if worker is None:
                return self.reply(404, {"error": "Worker not found"})
            if worker.get("outstanding", 0):
                return self.reply(409, {"error": "Worker is busy; wait for its subtasks to finish"})
            try:
                remove_worker_locked(wid)
                info(f"*** Removed {wid}\n")
                return self.reply(200, {"removed": wid})
            except Exception as exc:
                return self.reply(500, {"error": f"{type(exc).__name__}: {exc}"})


def main():
    global net, switch, manager_proc
    setLogLevel("info")
    info("*** Starting DST Mininet network\n")

    net = Mininet(
        controller=None,
        switch=OVSSwitch,
        link=TCLink,
        autoSetMacs=True,
        autoStaticArp=True,
    )
    switch = net.addSwitch("s1", protocols="OpenFlow13")
    manager = net.addHost("h1", ip=f"{MANAGER_IP}/24")
    net.addLink(manager, switch, cls=TCLink)
    net.start()

    # This build uses OVS standalone learning-switch mode, not a Ryu controller.
    subprocess.run(["ovs-vsctl", "set-fail-mode", "s1", "standalone"], check=True)
    configure_desktop_interface()

    server = ThreadingHTTPServer((DESKTOP_IP, AGENT_PORT), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()

    manager_proc = manager.popen(
        [PYTHON, "-u", "-m", "src.manager"],
        cwd=str(ROOT),
        stdout=None,
        stderr=None,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )

    info("\n*** DST NETWORK STARTED\n")
    info(f"*** Dashboard: http://{MANAGER_IP}:{DASHBOARD_PORT}\n")
    info("*** Add/remove real Mininet workers from the dashboard. Ctrl+C to stop.\n")

    try:
        while manager_proc.poll() is None:
            time.sleep(0.5)
    except KeyboardInterrupt:
        info("*** Shutting down DST network\n")
    finally:
        server.shutdown()
        server.server_close()
        with lock:
            for wid in list(active):
                try:
                    remove_worker_locked(wid)
                except Exception as exc:
                    info(f"*** Shutdown cleanup warning for {wid}: {exc}\n")
        stop_process(manager_proc)
        if net is not None:
            net.stop()
        subprocess.run(
            ["ovs-vsctl", "--if-exists", "del-port", "s1", "host0"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        info("*** DST network stopped\n")


if __name__ == "__main__":
    main()
