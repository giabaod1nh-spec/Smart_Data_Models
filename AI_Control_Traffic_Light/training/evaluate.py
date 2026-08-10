"""Compare fixed vs independent vs cooperative DQN on same scenarios."""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

from AI_Control_Traffic_Light.config.settings import load_rl_config
from AI_Control_Traffic_Light.environment.traffic_environment import TrafficEnvironment
from AI_Control_Traffic_Light.utils.path_setup import ensure_paths
from AI_Control_Traffic_Light.utils.seed import set_global_seed

log = logging.getLogger(__name__)


def main(argv: list | None = None) -> int:
    ensure_paths()
    parser = argparse.ArgumentParser(description="Evaluate traffic control modes")
    parser.add_argument("--scenario", default="spillback")
    parser.add_argument("--episodes", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--modes", nargs="+", default=["fixed", "independent", "cooperative"])
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_rl_config()
    set_global_seed(args.seed)

    rows = []
    for mode in args.modes:
        env = TrafficEnvironment(config, mode=mode, use_gui=False)
        try:
            for ep in range(1, args.episodes + 1):
                m = env.run_episode(ep, args.scenario, train=False)
                rows.append(
                    {
                        "mode": mode,
                        "episode": ep,
                        "global_reward": m.global_reward,
                        "avg_waiting": m.avg_waiting,
                        "avg_queue": m.avg_queue,
                        "throughput": m.throughput,
                        "spillback_events": m.spillback_events,
                    }
                )
                log.info(
                    "%s ep=%s reward=%.1f queue=%.1f spillback=%s",
                    mode, ep, m.global_reward, m.avg_queue, m.spillback_events,
                )
        finally:
            env.close()

    out = args.output or config.models_dir / "evaluation_results.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)
    log.info("Results written to %s", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
