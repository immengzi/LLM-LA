# main_sim.py
from __future__ import annotations

import argparse
from pathlib import Path

from config import load_config
from sim.runner import run_sim_from_yaml


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help=(
            "Config selector. Same behavior as main.py: "
            "bare name loads ./configs/<name>.yaml else treated as path."
        ),
    )
    parser.add_argument("--n", type=int, default=None, help="Override total_requests (optional)")
    args = parser.parse_args()

    if args.config:
        raw = Path(args.config)
        if raw.parent != Path(".") or raw.suffix:
            config_path = raw
        else:
            config_path = Path("configs") / f"{args.config}.yaml"
    else:
        config_path = Path("configs") / "2-example_config.yaml"

    cfg = load_config(str(config_path))
    if args.n is not None:
        cfg.total_requests = int(args.n)

    run_sim_from_yaml(config_path=str(config_path), cfg=cfg)


if __name__ == "__main__":
    main()
