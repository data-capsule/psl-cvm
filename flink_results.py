import matplotlib
import matplotlib.pyplot as plt
import re
from pathlib import Path
from collections import OrderedDict

RUNTIME_RGX = re.compile(r"Job Runtime:\s*([0-9]+)\s*ms")

def parse_runtime_ms(log_path: Path) -> int | None:
    """
    Return the last 'Job Runtime: N ms' found in flink.log, or None if absent.
    """
    try:
        last_ms = None
        with log_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                m = RUNTIME_RGX.search(line)
                if m:
                    last_ms = int(m.group(1))
        return last_ms
    except FileNotFoundError:
        return None

def collect_flink_runtimes(top_level_dir: str):
    """
    Walk experiments/{flink,flink_no_psl}/{0,1,2,...}/logs/0/flink.log
    Build two dicts: {workers -> runtime_ms}.
    workers = 2**i for directory name i.
    """
    top = Path(top_level_dir).expanduser().resolve()
    suites = ["flink", "flink_no_psl"]
    result = {suite: {} for suite in suites}

    for suite in suites:
        suite_dir = top / "experiments" / suite
        if not suite_dir.is_dir():
            continue

        # Visit numeric subdirs only, e.g., 0,1,2,3,...
        for d in sorted((p for p in suite_dir.iterdir() if p.is_dir() and p.name.isdigit()),
                        key=lambda p: int(p.name)):
            idx = int(d.name)
            workers = 2 ** idx
            log_path = d / "logs" / "0" / "flink.log"
            ms = parse_runtime_ms(log_path)
            if ms is not None:
                result[suite][workers] = ms
            else:
                # If desired, try a fallback or just skip
                # e.g., parse from flink.err or another repeat
                pass

        # Make it ordered by workers for nice printing
        result[suite] = OrderedDict(sorted(result[suite].items()))

    return result["flink"], result["flink_no_psl"]

def plot_flink_compare(flink_map, flink_no_psl_map, output=None, width=30, height=12):
    font = {
        'size'   : 90,
        'family': 'serif',
        'serif': ["Linux Libertine O"],
    }
    matplotlib.rc('font', **font)
    matplotlib.rc("axes.formatter", limits=(-99, 99))
    matplotlib.rcParams['ps.useafm'] = True
    matplotlib.rcParams['pdf.use14corefonts'] = True
    matplotlib.rcParams['text.usetex'] = True
    # matplotlib.rcParams["text.latex.preview"] = True
    matplotlib.rcParams['text.latex.preamble'] = r"""
    \usepackage{libertine}
    \usepackage[libertine]{newtxmath}
    """

    plt.gcf().set_size_inches(30, 12)

    for experiment_name, experiment in [("HarborMaster", flink_map), ("Unprotected", flink_no_psl_map)]:
        x = list(experiment.keys())
        y = list(experiment.values())
        y = [int(y)/1000 for y in y] # Convert to seconds

        all_points = list(zip(x, y))
        all_points.sort(key=lambda x: x[0])
        x, y = zip(*all_points)
        if experiment_name == "HarborMaster":
            marker = '^'
        else:
            marker = 'o'
        plt.plot(x, y, marker=marker, linewidth=10, markersize=30, label=experiment_name)
        plt.xlabel("Number of Flink Workers")
        plt.ylabel("Completion Time (s)")
        plt.xscale('log')
        # plt.grid(True)
        plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.25), ncol=2, fontsize=70)
        # plt.yscale('log')
        plt.xticks(x, [str(int(x)) for x in x])
        plt.yticks([100, 200, 400, 600, 800, 900])
    plt.grid()
    plt.savefig("flink_experiment.pdf", bbox_inches="tight")
    plt.show()

if __name__ == "__main__":
    flink_map, flink_no_psl_map = collect_flink_runtimes("deployment_artifacts/2025-10-26T16:39:18.792631+00:00")
    print(flink_map)
    print(flink_no_psl_map)
    plot_flink_compare(flink_map, flink_no_psl_map, output="flink_experiment.pdf")