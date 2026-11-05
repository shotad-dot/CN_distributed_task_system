"""Flask dashboard + coordinator. Runs inside Mininet manager host h1."""
import json
import socket
import threading
import time
import uuid
import statistics
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from pathlib import Path

from flask import Flask, jsonify, render_template
from src.scheduler import WorkloadAwareScheduler

ROOT = Path(__file__).resolve().parent.parent
app = Flask(__name__, template_folder=str(ROOT / "templates"),
            static_folder=str(ROOT / "static"))

AGENT = "http://10.0.0.254:8765"
PORT = 5000
MATRIX_SIZE = 300  # Larger CPU-bound workload; 24x24 is too small to benefit from distribution.

workers = {}
wlock = threading.RLock()
jobs = deque(maxlen=80)
jlock = threading.RLock()
scheduler = WorkloadAwareScheduler()
metrics = {
    "jobs": 0, "completed": 0, "failed": 0, "subtasks": 0,
    "latencies": deque(maxlen=300), "recent": deque(maxlen=300)
}


def agent(method, path):
    req = urllib.request.Request(
        AGENT + path, method=method,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=35) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(exc.read().decode()) from exc


def tcp(worker, payload):
    body = (json.dumps(payload, separators=(",", ":")) + "\n").encode()
    start = time.perf_counter()

    with socket.create_connection((worker["ip"], PORT), timeout=5) as sock:
        sock.settimeout(180)
        sock.sendall(body)
        raw = bytearray()

        while b"\n" not in raw and len(raw) < 64_000_000:
            chunk = sock.recv(65536)
            if not chunk:
                break
            raw.extend(chunk)

    if not raw:
        raise RuntimeError("Worker closed the connection without a response.")

    answer = json.loads(raw.decode().strip())
    if not answer.get("ok"):
        raise RuntimeError(answer.get("error", "Worker failed"))

    return answer["result"], time.perf_counter() - start


def snapshot():
    with wlock:
        return [dict(worker) for worker in workers.values()]


def mark_worker_failed(worker_id, reason):
    """Mark a failed worker unavailable locally and stop its process via the Mininet agent."""
    with wlock:
        current = workers.get(worker_id)
        if current:
            current["status"] = "FAILED"
            current["outstanding"] = 0
            current["last_error"] = str(reason)
            current["failed"] = current.get("failed", 0) + 1
    try:
        agent("POST", f"/workers/{worker_id}/fail")
    except Exception:
        # The worker may already be dead; local exclusion is still essential.
        pass


def run_chunks(job_id, kind, chunks):
    """Execute chunks concurrently; retry an unfinished chunk on another worker."""
    initial_pool = [w for w in snapshot() if w["status"] == "ACTIVE"]
    if not initial_pool:
        raise RuntimeError("Add at least one active worker first.")

    records = []
    results = [None] * len(chunks)
    records_lock = threading.Lock()

    def dispatch(index, chunk):
        task_id = f"{job_id}-C{index + 1}"
        tried = []
        attempt_log = []
        last_error = "No available worker"

        # A chunk is retried on each remaining ACTIVE worker at most once.
        while True:
            with wlock:
                candidates = [
                    dict(w) for w in workers.values()
                    if w["status"] == "ACTIVE" and w["id"] not in tried
                ]
                if not candidates:
                    break
                chosen = scheduler.choose(candidates)
                tried.append(chosen["id"])
                workers[chosen["id"]]["outstanding"] += 1

            started = time.perf_counter()
            try:
                value, elapsed = tcp(
                    chosen, {"id": task_id, "kind": kind, "data": chunk}
                )
                results[index] = value
                with wlock:
                    current = workers.get(chosen["id"])
                    if current:
                        current["outstanding"] = max(0, current["outstanding"] - 1)
                        current["ewma_seconds"] = .3 * elapsed + .7 * current["ewma_seconds"]
                        current["completed"] += 1
                attempt_log.append({
                    "worker": chosen["name"], "worker_id": chosen["id"],
                    "status": "COMPLETED", "reason": "",
                    "duration_ms": round(elapsed * 1000, 2)
                })
                record = {
                    "id": task_id, "worker": chosen["name"],
                    "worker_id": chosen["id"], "worker_ip": chosen["ip"],
                    "status": "COMPLETED", "result": "partial result ready",
                    "duration_ms": round(elapsed * 1000, 2),
                    "attempts": len(attempt_log), "attempt_log": attempt_log,
                    "reallocated": len(attempt_log) > 1
                }
                with records_lock:
                    records.append(record)
                return "COMPLETED"

            except Exception as exc:
                elapsed = time.perf_counter() - started
                last_error = f"{type(exc).__name__}: {exc}"
                attempt_log.append({
                    "worker": chosen["name"], "worker_id": chosen["id"],
                    "status": "FAILED", "reason": last_error,
                    "duration_ms": round(elapsed * 1000, 2)
                })
                mark_worker_failed(chosen["id"], last_error)
                # Continue: the same unfinished chunk is now eligible for another worker.

        record = {
            "id": task_id, "worker": " → ".join(a["worker"] for a in attempt_log),
            "worker_id": "", "worker_ip": "", "status": "FAILED",
            "result": last_error, "duration_ms": 0,
            "attempts": len(attempt_log), "attempt_log": attempt_log,
            "reallocated": len(attempt_log) > 1
        }
        with records_lock:
            records.append(record)
        return "FAILED"

    with ThreadPoolExecutor(max_workers=max(1, min(32, len(chunks)))) as executor:
        futures = [executor.submit(dispatch, i, chunk) for i, chunk in enumerate(chunks)]
        statuses = [future.result() for future in as_completed(futures)]

    if "FAILED" in statuses:
        error = RuntimeError(
            "A subtask could not be completed: all available workers failed. "
            "See the subtask trace for attempts and reallocation history."
        )
        error.trace = sorted(records, key=lambda record: record["id"])
        raise error

    return results, sorted(records, key=lambda record: record["id"])


def matrix_job(n=MATRIX_SIZE):
    """Create a deterministic square matrix job split into balanced row blocks."""
    active_workers = [
        worker for worker in snapshot() if worker["status"] == "ACTIVE"
    ]
    if not active_workers:
        raise RuntimeError("Add at least one worker before submitting a job.")

    a = [[(i * 3 + j * 2) % 9 for j in range(n)] for i in range(n)]
    b = [[(i + j * 4) % 7 for j in range(n)] for i in range(n)]

    # Exactly one block per active worker (up to n workers), with balanced rows.
    block_count = min(len(active_workers), n)
    base_rows, extra_rows = divmod(n, block_count)
    chunks = []
    start_row = 0

    for block_index in range(block_count):
        row_count = base_rows + (1 if block_index < extra_rows else 0)
        end_row = start_row + row_count
        chunks.append({
            "a": a,
            "b": b,
            "row_range": [start_row, end_row]
        })
        start_row = end_row

    return {
        "chunks": chunks,
        "finalize": lambda parts: [
            row for part in parts for row in part
        ],
        "description": f"{n}x{n} matrix multiplication ({block_count} row blocks)"
    }


def create_job(kind):
    job_id = "J-" + uuid.uuid4().hex[:8].upper()
    started = time.perf_counter()

    try:
        if kind == "matrix":
            spec = matrix_job(MATRIX_SIZE)
            chunk_kind = "matrix_block"

        elif kind == "sum":
            data = list(range(1, 100001))
            size = 10000
            spec = {
                "chunks": [
                    data[index:index + size]
                    for index in range(0, len(data), size)
                ],
                "finalize": sum,
                "description": "Sum integers 1 to 100,000"
            }
            chunk_kind = "sum"

        elif kind == "primes":
            upper = 2_000_000
            number_of_chunks = 2
            width = (upper - 1 + number_of_chunks - 1) // number_of_chunks
            chunks = []

            for index in range(number_of_chunks):
                low = 2 + index * width
                high = min(upper, low + width - 1)
                if low <= high:
                    chunks.append([low, high])

            spec = {
                "chunks": chunks,
                "finalize": lambda parts: sorted(
                    prime for part in parts for prime in part
                ),
                "description": (
                    "Find all primes up to 2,000,000 (2 fixed chunks)"
                )
            }
            chunk_kind = "primes"

        elif kind == "sort":
            data = [((index * 7919) % 100003) for index in range(60000)]
            size = 6000
            spec = {
                "chunks": [
                    data[index:index + size]
                    for index in range(0, len(data), size)
                ],
                "finalize": lambda parts: sorted(
                    value for part in parts for value in part
                ),
                "description": (
                    "Sort 60,000 integers using parallel chunks + merge"
                )
            }
            chunk_kind = "sort_chunk"

        else:
            raise ValueError("Unknown sample job")

        parts, trace = run_chunks(job_id, chunk_kind, spec["chunks"])
        result = spec["finalize"](parts)
        status = "COMPLETED"
        error_message = None

        # Keep the dashboard response compact for the larger matrix result.
        if kind == "matrix":
            result_for_dashboard = {
                "rows": len(result),
                "columns": len(result[0]) if result else 0,
                "first_row_sample": result[0][:8] if result else [],
                "checksum": sum(sum(row) for row in result)
            }
        else:
            result_for_dashboard = result

    except Exception as exc:
        result_for_dashboard = None
        trace = getattr(exc, "trace", locals().get("trace", []))
        status = "FAILED"
        error_message = str(exc)
        spec = locals().get("spec", {})

    duration = time.perf_counter() - started
    job = {
        "id": job_id,
        "type": kind,
        "description": locals().get("spec", {}).get("description", kind),
        "status": status,
        "subtasks": len(locals().get("spec", {}).get("chunks", [])),
        "duration_ms": round(duration * 1000, 2),
        "result": result_for_dashboard,
        "error": error_message,
        "trace": trace,
        "created": time.strftime("%H:%M:%S")
    }

    with jlock:
        jobs.appendleft(job)

    metrics["jobs"] += 1
    metrics["completed" if status == "COMPLETED" else "failed"] += 1
    metrics["subtasks"] += job["subtasks"]
    metrics["latencies"].append(duration)
    metrics["recent"].append(time.time())

    return job


@app.get("/")
def home():
    return render_template("index.html")


@app.get("/api/state")
def state():
    with jlock:
        job_list = list(jobs)

    worker_list = snapshot()
    active = [
        worker for worker in worker_list if worker["status"] == "ACTIVE"
    ]
    loads = [worker["outstanding"] for worker in active]
    average = (
        statistics.mean(metrics["latencies"])
        if metrics["latencies"] else 0
    )
    balance = (
        max(
            0,
            100 * (
                1 - statistics.pstdev(loads) / (statistics.mean(loads) + 1)
            )
        )
        if len(loads) > 1 else (100 if active else 0)
    )
    success = (
        100 * metrics["completed"] / max(1, metrics["jobs"])
    )
    throughput = sum(
        time.time() - timestamp < 60 for timestamp in metrics["recent"]
    )

    return jsonify(
        workers=worker_list,
        jobs=job_list,
        metrics={
            "active_workers": len(active),
            "jobs": metrics["jobs"],
            "completed": metrics["completed"],
            "failed": metrics["failed"],
            "subtasks": metrics["subtasks"],
            "avg_latency_ms": round(average * 1000, 2),
            "throughput_per_minute": throughput,
            "load_balance_pct": round(balance, 1),
            "success_rate_pct": round(success, 1)
        }
    )


@app.post("/api/workers")
def add_worker():
    try:
        worker = agent("POST", "/workers")
        with wlock:
            workers[worker["id"]] = worker
        return jsonify(worker), 201
    except Exception as exc:
        return jsonify(error=str(exc)), 500


@app.post("/api/workers/<wid>/fail")
def fail_worker(wid):
    with wlock:
        if wid not in workers:
            return jsonify(error="Worker not found"), 404
        if workers[wid]["status"] != "ACTIVE":
            return jsonify(error="Worker is already failed or inactive"), 409
    try:
        result = agent("POST", f"/workers/{wid}/fail")
    except Exception as exc:
        return jsonify(error=str(exc)), 500
    with wlock:
        current = workers.get(wid)
        if current:
            current["status"] = "FAILED"
            current["last_error"] = "Failure injected from dashboard"
    return jsonify(result)


@app.delete("/api/workers/<wid>")
def remove_worker(wid):
    with wlock:
        if wid not in workers:
            return jsonify(error="Worker not found"), 404
        if workers[wid]["outstanding"]:
            return jsonify(
                error="Worker has active subtasks; wait for completion."
            ), 409

    try:
        agent("DELETE", "/workers/" + wid)
    except Exception as exc:
        return jsonify(error=str(exc)), 500

    with wlock:
        workers.pop(wid, None)

    return jsonify(removed=wid)


@app.post("/api/jobs/<kind>")
def launch_job(kind):
    job = create_job(kind)
    return jsonify(job), 200 if job["status"] == "COMPLETED" else 502


@app.get("/health")
def health():
    return jsonify(status="ok")


if __name__ == "__main__":
    print("Manager dashboard: http://10.0.0.1:8000", flush=True)
    app.run(
        host="0.0.0.0", port=8000,
        threaded=True, use_reloader=False
    )
