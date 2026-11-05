"""Line-delimited JSON TCP worker. One request contains one independent chunk."""
import json
import socket
import sys


def run_task(task):
    kind = task["kind"]
    data = task["data"]

    if kind == "sum":
        return sum(data)

    if kind == "matrix_block":
        a = data["a"]
        b = data["b"]
        row_start, row_end = data["row_range"]

        # Compute only the assigned rows; each worker returns its row block.
        b_columns = list(zip(*b))
        return [
            [
                sum(a[row][k] * column[k] for k in range(len(a[row])))
                for column in b_columns
            ]
            for row in range(row_start, row_end)
        ]

    if kind == "primes":
        low, high = data

        def is_prime(number):
            if number < 2:
                return False
            if number % 2 == 0:
                return number == 2
            divisor = 3
            while divisor * divisor <= number:
                if number % divisor == 0:
                    return False
                divisor += 2
            return True

        return [
            number for number in range(low, high + 1)
            if is_prime(number)
        ]

    if kind == "sort_chunk":
        return sorted(data)

    raise ValueError("Unknown task kind: " + str(kind))


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", port))
    server.listen(64)

    print(f"Worker listening on 0.0.0.0:{port}", flush=True)

    while True:
        connection, address = server.accept()

        with connection:
            try:
                raw = bytearray()

                while b"\n" not in raw and len(raw) < 64_000_000:
                    chunk = connection.recv(65536)
                    if not chunk:
                        break
                    raw.extend(chunk)

                if not raw:
                    continue

                task = json.loads(raw.decode().strip())
                result = run_task(task)
                response = {"ok": True, "result": result}

            except Exception as exc:
                response = {"ok": False, "error": str(exc)}

            try:
                connection.sendall(
                    (json.dumps(response, separators=(",", ":")) + "\n").encode()
                )
            except (BrokenPipeError, ConnectionResetError):
                print("Client disconnected before receiving the result.",
                      flush=True)


if __name__ == "__main__":
    main()
