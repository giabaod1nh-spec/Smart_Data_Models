"""Collect final isolated runtime evidence; no state mutation."""
import json
import subprocess
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen

def main():
    root=Path("de/artifacts/v1_raw/acceptance/phase1-isolated-long01")
    root.mkdir(parents=True,exist_ok=True)
    base=["docker","compose","-p","phase1acceptance","-f","docker-compose.v1-acceptance.yml"]
    commands={"compose-config.txt":base+["config"],
              "compose-ps.txt":base+["ps","--all"],
              "worker-logs.txt":base+["logs","--no-color","raw"],
              "broker-offsets.txt":base+["exec","-T","kafka","/opt/kafka/bin/kafka-consumer-groups.sh",
                  "--bootstrap-server","kafka:9092","--describe","--group","de-kafka-raw-v1"],
              "resource-snapshot.txt":["docker","stats","--no-stream","--format","{{.Name}} {{.CPUPerc}} {{.MemUsage}}",
                                      "phase1acceptance-raw-1","phase1acceptance-kafka-1","phase1acceptance-clickhouse-1"],
              "image.txt":["docker","inspect","--format","{{.Image}}","phase1acceptance-raw-1"],
              "git-status.txt":["git","status","--short","--branch"]}
    for name,command in commands.items():
        result=subprocess.run(command,capture_output=True,text=True,check=True)
        (root/name).write_text(result.stdout+result.stderr,encoding="utf-8")
    h=json.load(urlopen("http://localhost:18097/health"))
    assert h["ready"] and len(h["assigned"])==3
    assert all(p["lag"]==0 for p in h["partition_offsets"])
    (root/"health-final.json").write_text(json.dumps(h,indent=2),encoding="utf-8")
    report=json.loads((root/"reconciliation.json").read_text())
    start,end,count=report["processing_interval"][0]
    elapsed=(datetime.fromisoformat(end)-datetime.fromisoformat(start)).total_seconds()
    performance=dict(simulation_seconds=1200,sumo_step_length=.1,complete_cycles=report["complete_cycles"],
          raw_deliveries=count,observed_seconds=elapsed,observed_deliveries_per_second=count/elapsed,
          final_lag=[p["lag"] for p in h["partition_offsets"]],
          final_buffer=h["buffered_records"],note="Observed producer workload, not peak capacity benchmark.")
    (root/"performance.json").write_text(json.dumps(performance,indent=2),encoding="utf-8")
    print(json.dumps(performance,indent=2))

if __name__=="__main__":
    main()

