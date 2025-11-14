#!/usr/bin/env python3
import os, sys, argparse, subprocess, shlex
import pandas as pd
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.abspath(__file__))
BIN_DIR = os.path.join(ROOT, "bin")
OUT_DIR = os.path.join(ROOT, "out")
PYTHON = sys.executable

PY_FILE = os.path.join(ROOT, "python", "concurrency_demo.py")
GO_SRC = os.path.join(ROOT, "go", "concurrency_demo.go")
C_SRC = os.path.join(ROOT, "c", "concurrency_demo.c")

GO_BIN = os.path.join(BIN_DIR, "concurrency_demo")
C_BIN = os.path.join(BIN_DIR, "concurrency_demo_c")

PY_CSV = os.path.join(OUT_DIR, "python_results.csv")
GO_CSV = os.path.join(OUT_DIR, "go_results.csv")
C_CSV = os.path.join(OUT_DIR, "c_results.csv")

COMBINED_PNG1 = os.path.join(OUT_DIR, "combined_success_vs_workers.png")
COMBINED_PNG2 = os.path.join(OUT_DIR, "combined_spread_vs_workers.png")
COMBINED_SUBPLOTS = os.path.join(OUT_DIR, "combined_spread_subplots.png")


def shell(cmd_list):
    cmd = " ".join(shlex.quote(x) for x in cmd_list)
    print(">>", cmd)
    subprocess.check_call(cmd_list)


def build_go():
    if not os.path.exists(GO_BIN):
        print("[go] building…")
        shell(["go", "build", "-o", GO_BIN, GO_SRC])
    else:
        print("[go] binary exists, skipping build")


def build_c():
    if not os.path.exists(C_BIN):
        print("[c ] building…")
        shell(["gcc", "-O2", "-pthread", "-o", C_BIN, C_SRC])
    else:
        print("[c ] binary exists, skipping build")


def run_python(workers, reps):
    shell(
        [
            PYTHON,
            PY_FILE,
            "--workers",
            workers,
            "--reps",
            str(reps),
            "--csv",
            PY_CSV,
            "--png1",
            os.path.join(OUT_DIR, "py_success.png"),
            "--png2",
            os.path.join(OUT_DIR, "py_spread.png"),
        ]
    )


def run_go(workers, reps):
    shell([GO_BIN, "-workers", workers, "-reps", str(reps), "-csv", GO_CSV])


def run_c(workers, reps):
    shell([C_BIN, "-workers", workers, "-reps", str(reps), "-csv", C_CSV])


def combined_plots():
    def has(df, col):
        return col in df.columns

    df_py = pd.read_csv(PY_CSV)
    df_go = pd.read_csv(GO_CSV)
    df_c = pd.read_csv(C_CSV)

    # Success: avg and p99 only (p99 optional)
    plt.figure(figsize=(9, 6))
    for df, label in [(df_py, "Python"), (df_go, "Go"), (df_c, "C")]:
        x = df["workers"]
        if has(df, "avg_success_rate"):
            plt.plot(
                x, df["avg_success_rate"] * 100.0, marker="o", label=f"{label} avg"
            )
        if has(df, "p99_success_rate"):
            plt.plot(
                x,
                df["p99_success_rate"] * 100.0,
                marker="^",
                linestyle=":",
                label=f"{label} p99",
            )
    plt.xlabel("Workers pulling simultaneously")
    plt.ylabel("Success rate (%)")
    plt.ylim(95, 101)
    plt.grid(True, alpha=0.3)
    plt.title("Success Rate vs Workers (avg and p99)")
    plt.legend(ncol=2)
    plt.tight_layout()
    plt.savefig(COMBINED_PNG1, dpi=140)

    # Spread: avg and p99 only (p99 optional)
    plt.figure(figsize=(9, 6))
    for df, label in [(df_py, "Python"), (df_go, "Go"), (df_c, "C")]:
        x = df["workers"]
        if has(df, "avg_spread_ms"):
            plt.plot(x, df["avg_spread_ms"], marker="o", label=f"{label} avg")
        if has(df, "p99_spread_ms"):
            plt.plot(
                x, df["p99_spread_ms"], marker="^", linestyle=":", label=f"{label} p99"
            )
    plt.xlabel("Workers pulling simultaneously")
    plt.ylabel("Spread (ms)")
    plt.grid(True, alpha=0.3)
    plt.title("Pull Spread vs Workers (avg and p99)")
    plt.legend(ncol=2)
    plt.tight_layout()
    plt.savefig(COMBINED_PNG2, dpi=140)

    print(f"\nCombined plots saved:\n- {COMBINED_PNG1}\n- {COMBINED_PNG2}")


def combined_spread_subplots():
    df_py = pd.read_csv(PY_CSV)
    df_go = pd.read_csv(GO_CSV)
    df_c = pd.read_csv(C_CSV)
    dfs = [(df_py, "Python"), (df_go, "Go"), (df_c, "C")]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)

    panels = [
        ("avg_spread_ms", "Average spread (ms)"),
        ("p99_spread_ms", "p99 spread (ms)"),
    ]

    for ax, (col, title) in zip(axes, panels):
        for df, label in dfs:
            if col in df.columns:
                ax.plot(df["workers"], df[col], marker="o", label=label)
        ax.set_xlabel("Workers")
        ax.set_title(title)
        ax.grid(True, alpha=0.3)

    axes[0].set_ylabel("Spread (ms)")
    axes[0].legend()
    plt.tight_layout()
    plt.savefig(COMBINED_SUBPLOTS, dpi=140)
    print(f"Saved {COMBINED_SUBPLOTS}")


def parse_args():
    ap = argparse.ArgumentParser(
        description="Build and run the concurrency demos, then plot results."
    )
    ap.add_argument(
        "--workers",
        type=str,
        default="10,20,40,80,160,320",
        help="Comma-separated worker counts to test, e.g. 10,50,100,...,1000",
    )
    ap.add_argument("--reps", type=int, default=20, help="Trials per worker count")
    return ap.parse_args()


def main():
    args = parse_args()
    os.makedirs(BIN_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)

    build_go()
    build_c()

    run_python(args.workers, args.reps)
    run_go(args.workers, args.reps)
    run_c(args.workers, args.reps)

    combined_plots()
    combined_spread_subplots()


if __name__ == "__main__":
    main()
