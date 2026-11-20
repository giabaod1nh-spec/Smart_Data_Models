"""Reproducible host SUMO acceptance with isolated producer artifacts."""
import argparse
import os
import sys
from pathlib import Path

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--run-id",required=True)
    p.add_argument("--duration",type=float,default=1200)
    p.add_argument("--step-length",type=float,default=.1)
    p.add_argument("--bootstrap",default="localhost:39092")
    p.add_argument("--artifact-root",default="de/artifacts/v1_raw/producer-isolated")
    args=p.parse_args()
    root=Path(__file__).resolve().parents[2]
    artifacts=(root/args.artifact_root).resolve()
    os.environ["SUMO_STEP_LENGTH"]=str(args.step_length)
    os.environ["KAFKA_BOOTSTRAP_SERVERS"]=args.bootstrap
    sys.path.insert(0,str(root/"Visualize"))
    from configuration import config
    config.ARTIFACTS_DIR=artifacts
    config.ARTIFACT_ROOT=artifacts
    config.V1_OUTBOX_DB=artifacts/"kafka_outbox"/"v1-outbox.sqlite3"
    from app.traci_runner import run,build_parser
    ns=build_parser().parse_args(["--no-gui","--no-orion","--no-api",
        "--v1-observations","--v1-publish","--nodes","A,B,C,D,M",
        "--simulation-run-id",args.run_id,"--max-sim-time",str(args.duration),
        "--log-level","WARNING"])
    raise SystemExit(run(ns))

if __name__=="__main__":
    main()

