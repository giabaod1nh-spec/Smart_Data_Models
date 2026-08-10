"""CLI: train cooperative / independent multi-agent DQN."""
from __future__ import annotations

import argparse
import logging
import sys

from AI_Control_Traffic_Light.config.settings import load_rl_config
from AI_Control_Traffic_Light.environment.traffic_environment import TrafficEnvironment
from AI_Control_Traffic_Light.utils.path_setup import ensure_paths
from AI_Control_Traffic_Light.utils.seed import set_global_seed

log = logging.getLogger(__name__)


def main(argv: list | None = None) -> int:
    ensure_paths()
    parser = argparse.ArgumentParser(description="Train multi-agent DQN traffic control")
    parser.add_argument("--mode", choices=["fixed", "independent", "cooperative"], default="cooperative")
    parser.add_argument("--episodes", type=int, default=None)
    parser.add_argument("--scenario", default="heavy_traffic")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--gui", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = load_rl_config()
    if args.episodes is not None:
        config.num_episodes = args.episodes
    if args.seed is not None:
        config.seed = args.seed

    set_global_seed(config.seed)
    env = TrafficEnvironment(config, mode=args.mode, use_gui=args.gui)

    try:
        for ep in range(1, config.num_episodes + 1):
            train = args.mode != "fixed"
            metrics = env.run_episode(ep, args.scenario, train=train)
            if train:
                if ep % config.target_update_frequency == 0:
                    env.manager.update_targets()
                env.manager.decay_epsilon_all()
                if ep % 10 == 0 or ep == 1:
                    env.manager.save_models()
            log.info(
                "Episode %s | mode=%s | global_reward=%.1f | avg_queue=%.1f | spillback=%s",
                ep,
                args.mode,
                metrics.global_reward,
                metrics.avg_queue,
                metrics.spillback_events,
            )
        if args.mode != "fixed":
            env.manager.save_models()
    finally:
        env.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
