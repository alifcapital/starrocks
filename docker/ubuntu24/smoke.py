#!/usr/bin/env python3
"""Check shared-data writes, reads, Java 21 UDFs, and restart persistence."""
import argparse
import functools
import http.server
import json
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request

MYSQL = ["mysql", "-h127.0.0.1", "-P9030", "-uroot", "--batch", "--raw", "--skip-column-names"]


def sql(statement):
    return subprocess.check_output(MYSQL + ["-e", statement], text=True, timeout=180).strip()


def check(statement, expected):
    actual = sql(statement)
    if actual != expected:
        raise AssertionError(f"{statement}\nExpected {expected!r}, got {actual!r}")


def ports_ready():
    for port in (9030, 9060, 9070):
        with socket.create_connection(("127.0.0.1", port), timeout=5):
            pass


def metrics():
    with urllib.request.urlopen("http://127.0.0.1:8040/metrics", timeout=15) as response:
        lines = response.read().decode().splitlines()
    return [line for line in lines if not line.startswith("#") and re.search("starlet|starcache|datacache", line)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=360)
    parser.add_argument("--output", type=Path, default=Path.home() / "build-logs/smoke.json")
    args = parser.parse_args()
    if args.duration < 360:
        parser.error("The smoke duration must be at least 360 seconds")
    repo = Path(__file__).resolve().parents[2]
    revision = subprocess.check_output(["git", "rev-parse", "--short=7", "HEAD"], cwd=repo, text=True).strip()
    db = "ubuntu24_smoke_" + str(int(time.time()))
    result = {"commit": revision, "database": db, "status": "RUNNING"}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    try:
        ports_ready()
        result["frontends"] = sql("SHOW FRONTENDS")
        result["compute_nodes"] = sql("SHOW COMPUTE NODES")
        for key in ("frontends", "compute_nodes"):
            if revision not in result[key]:
                raise AssertionError(f"{key} does not report source revision {revision}: {result[key]}")
        result["java"] = subprocess.check_output(["java", "--version"], text=True)
        if not result["java"].startswith("openjdk 21."):
            raise AssertionError("Host Java is not JDK 21")
        result["metrics_before"] = metrics()
        sql(f"CREATE DATABASE {db}")
        sql(f"CREATE TABLE {db}.data (id BIGINT NOT NULL, v INT, payload VARCHAR(100)) "
            "PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 4 "
            'PROPERTIES ("replication_num"="1", "datacache.enable"="true")')
        sql(f"INSERT INTO {db}.data SELECT generate_series, CAST(generate_series AS INT), "
            "CONCAT('row-', CAST(generate_series AS STRING)) FROM TABLE(generate_series(1,10000))")
        check(f"SELECT COUNT(*), SUM(v), COUNT(DISTINCT payload) FROM {db}.data", "10000\t50005000\t10000")
        with tempfile.TemporaryDirectory(prefix="sr-jdk21-udf-") as directory:
            root = Path(directory)
            (root / "SmokeAdd.java").write_text(
                "public class SmokeAdd { public Integer evaluate(Integer n) { "
                "return n == null ? null : n + 1; } }\n"
            )
            subprocess.run(["javac", "--release", "21", str(root / "SmokeAdd.java")], check=True)
            subprocess.run(["jar", "cf", str(root / "smoke.jar"), "-C", directory, "SmokeAdd.class"], check=True)
            handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=directory)
            # Both FE and the single CN run on this host.
            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                sql(f"CREATE FUNCTION {db}.plus_one(INT) RETURNS INT "
                    "symbol = 'SmokeAdd' type = 'StarrocksJar' "
                    f"file = 'http://127.0.0.1:{server.server_port}/smoke.jar'")
                check(f"SELECT SUM({db}.plus_one(v)) FROM {db}.data", "50015000")
                check(f"SELECT {db}.plus_one(CAST(NULL AS INT))", "NULL")
                # Read after a primary-key upsert, then verify the same data after restart.
                sql(f"INSERT INTO {db}.data VALUES (1,100,'updated')")
                expected_sum = 50005099
                check(f"SELECT COUNT(*), SUM(v) FROM {db}.data", f"10000\t{expected_sum}")
                subprocess.run([str(Path.home() / "bin/sr-cluster-stop")], check=True, timeout=180)
                subprocess.run([str(Path.home() / "bin/sr-cluster-start")], check=True, timeout=600)
                check(f"SELECT COUNT(*), SUM(v) FROM {db}.data", f"10000\t{expected_sum}")
                result["restart_persistence"] = "PASS"
                rounds = 0
                # Continue for six minutes after restart to catch delayed Starlet failures.
                watch_started = time.monotonic()
                while True:
                    ports_ready()
                    sql(f"INSERT INTO {db}.data VALUES (10000,{10001 + rounds},'row-10000')")
                    expected_sum += 1
                    check(f"SELECT COUNT(*), SUM(v) FROM {db}.data", f"10000\t{expected_sum}")
                    check(f"SELECT SUM({db}.plus_one(v)) FROM {db}.data", str(expected_sum + 10000))
                    rounds += 1
                    print(f"SQL/UDF round {rounds}: PASS", flush=True)
                    if time.monotonic() - watch_started >= args.duration:
                        break
                    time.sleep(30)
                result["rounds_after_restart"] = rounds
                result["seconds_after_restart"] = round(time.monotonic() - watch_started, 2)
            finally:
                server.shutdown()
                server.server_close()
        result["metrics_after"] = metrics()
        result["status"] = "PASS"
    except Exception as error:
        result["status"] = "FAIL"
        result["error"] = str(error)
        raise
    finally:
        result["elapsed_seconds"] = round(time.monotonic() - started, 2)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({k: v for k, v in result.items() if not k.startswith("metrics_")}, indent=2), flush=True)


if __name__ == "__main__":
    main()
