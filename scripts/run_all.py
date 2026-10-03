"""Convenience launcher: 3 consumers + 3 producers as subprocesses. Ctrl-C stops all.
Run from project root: python -m scripts.run_all"""
import subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODS = ["consumers.integrity_checker", "consumers.sla_monitor", "consumers.status_aggregator",
        "producers.station_producer", "producers.supplier_producer", "producers.team_status_producer"]
procs = []
try:
    for m in MODS:
        procs.append(subprocess.Popen([sys.executable, "-m", m], cwd=ROOT))
        time.sleep(1.5)
    print("all running - Ctrl-C to stop")
    while all(p.poll() is None for p in procs):
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    for p in procs:
        p.terminate()
