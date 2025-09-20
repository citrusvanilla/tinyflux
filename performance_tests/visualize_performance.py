#!/usr/bin/env python3
"""
TinyFlux Performance Visualization

Generates performance charts from benchmark results JSON file.
"""
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from typing import Dict, List, Any
import os

# Set style for clean, professional plots
plt.style.use("default")
plt.rcParams["figure.figsize"] = (12, 8)
plt.rcParams["font.size"] = 11
plt.rcParams["axes.labelsize"] = 12
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["legend.fontsize"] = 10


def load_results(file_path: str = "performance_results.json") -> Dict[str, Any]:
    """Load benchmark results from JSON file."""
    with open(file_path, "r") as f:
        return json.load(f)


def extract_write_performance(
    results: Dict[str, Any],
) -> Dict[str, Dict[str, List]]:
    """Extract write performance data for visualization."""
    write_data = {
        "sizes": [],
        "memory_individual": [],
        "memory_batch": [],
        "csv_individual": [],
        "csv_batch": [],
    }

    if "write_performance_scaling" in results:
        # Get database sizes (convert from string format like "10,000" to int)
        sizes = []
        for size_str in results["write_performance_scaling"]["memory"].keys():
            size_num = int(size_str.replace(",", ""))
            sizes.append(size_num)

        sizes.sort()
        write_data["sizes"] = sizes

        for size in sizes:
            size_str = f"{size:,}"

            # Memory storage data
            if size_str in results["write_performance_scaling"]["memory"]:
                memory_data = results["write_performance_scaling"]["memory"][
                    size_str
                ]

                individual_perf = memory_data["individual"]["writes_per_second"]
                write_data["memory_individual"].append(
                    individual_perf if isinstance(individual_perf, int) else 0
                )

                batch_perf = memory_data["batch"]["writes_per_second"]
                write_data["memory_batch"].append(
                    batch_perf if isinstance(batch_perf, int) else 0
                )

            # CSV storage data
            if size_str in results["write_performance_scaling"]["csv"]:
                csv_data = results["write_performance_scaling"]["csv"][size_str]

                individual_perf = csv_data["individual"]["writes_per_second"]
                write_data["csv_individual"].append(
                    individual_perf if isinstance(individual_perf, int) else 0
                )

                batch_perf = csv_data["batch"]["writes_per_second"]
                write_data["csv_batch"].append(
                    batch_perf if isinstance(batch_perf, int) else 0
                )

    return write_data


def extract_read_performance(results: Dict[str, Any]) -> Dict[str, Any]:
    """Extract read performance data for visualization."""
    read_data = {
        "sizes": [],
        "memory_point_lookup": [],
        "memory_range_query": [],
        "memory_field_filter": [],
        "memory_complex_query": [],
        "csv_point_lookup": [],
        "csv_range_query": [],
        "csv_field_filter": [],
        "csv_complex_query": [],
    }

    if "read_performance_scaling" in results:
        # Get database sizes
        sizes = []
        for size_str in results["read_performance_scaling"]["memory"].keys():
            size_num = int(size_str.replace(",", ""))
            sizes.append(size_num)

        sizes.sort()
        read_data["sizes"] = sizes

        query_types = [
            "point_lookup",
            "range_query",
            "field_filter",
            "complex_query",
        ]

        for size in sizes:
            size_str = f"{size:,}"

            # Memory storage
            if size_str in results["read_performance_scaling"]["memory"]:
                memory_data = results["read_performance_scaling"]["memory"][
                    size_str
                ]
                for qt in query_types:
                    if qt in memory_data:
                        qps = memory_data[qt]["qps"]
                        read_data[f"memory_{qt}"].append(qps)
                    else:
                        read_data[f"memory_{qt}"].append(0)

            # CSV storage
            if size_str in results["read_performance_scaling"]["csv"]:
                csv_data = results["read_performance_scaling"]["csv"][size_str]
                for qt in query_types:
                    if qt in csv_data:
                        qps = csv_data[qt]["qps"]
                        read_data[f"csv_{qt}"].append(qps)
                    else:
                        read_data[f"csv_{qt}"].append(0)

    return read_data


def plot_write_performance_scaling(
    write_data: Dict[str, Any], results: Dict[str, Any]
):
    """Create write performance scaling chart with separate charts by storage type."""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

    sizes = write_data["sizes"]
    x_pos = np.arange(len(sizes))
    size_labels = [
        f"{size//1000}K" if size < 1000000 else f"{size//1000000}M"
        for size in sizes
    ]

    # Chart 1: Memory Individual Writes
    ax1.bar(x_pos, write_data["memory_individual"], color="#2E8B57", alpha=0.8)
    ax1.set_xlabel("Database Size")
    ax1.set_ylabel("Writes per Second")
    ax1.set_title("Memory Storage: Individual Inserts (.insert)")
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(size_labels)
    ax1.grid(True, alpha=0.3)
    ax1.ticklabel_format(style='scientific', axis='y', scilimits=(0,0))

    # Chart 2: Memory Batch Writes
    ax2.bar(x_pos, write_data["memory_batch"], color="#2E8B57", alpha=0.8)
    ax2.set_xlabel("Database Size")
    ax2.set_ylabel("Writes per Second")
    ax2.set_title("Memory Storage: Batch Inserts (.insert_multiple)")
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(size_labels)
    ax2.grid(True, alpha=0.3)
    ax2.ticklabel_format(style='scientific', axis='y', scilimits=(0,0))

    # Chart 3: CSV Individual Writes
    csv_individual_filtered = [val if val > 0 else 0 for val in write_data["csv_individual"]]
    bars3 = ax3.bar(x_pos, csv_individual_filtered, color="#CD853F", alpha=0.8)
    ax3.set_xlabel("Database Size")
    ax3.set_ylabel("Writes per Second")
    ax3.set_title("CSV Storage: Individual Inserts (.insert)")
    ax3.set_xticks(x_pos)
    ax3.set_xticklabels(size_labels)
    ax3.grid(True, alpha=0.3)
    
    # Add "Skipped" text for zero values
    for i, (bar, val) in enumerate(zip(bars3, write_data["csv_individual"])):
        if val <= 0:
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(csv_individual_filtered)*0.02,
                    'Skipped\n(>3min)', ha='center', va='bottom', fontsize=8, color='red')

    # Chart 4: CSV Batch Writes
    ax4.bar(x_pos, write_data["csv_batch"], color="#CD853F", alpha=0.8)
    ax4.set_xlabel("Database Size")
    ax4.set_ylabel("Writes per Second")
    ax4.set_title("CSV Storage: Batch Inserts (.insert_multiple)")
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels(size_labels)
    ax4.grid(True, alpha=0.3)

    # Add system info
    system_info = results.get("system_info", {})
    fig.suptitle(
        f"TinyFlux Write Performance by Storage Type\n{system_info.get('platform', 'Unknown Platform')} - "
        f"Python {system_info.get('python_version', 'Unknown')} - "
        f"TinyFlux {results['test_config']['tinyflux_version']}",
        fontsize=16,
    )

    plt.tight_layout()
    plt.savefig("charts/write_performance_scaling.png", dpi=300, bbox_inches="tight")
    plt.close()
    print("📊 Generated: charts/write_performance_scaling.png")


def plot_read_performance_scaling(
    read_data: Dict[str, Any], results: Dict[str, Any]
):
    """Create read performance scaling chart."""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

    sizes = read_data["sizes"]
    x_pos = np.arange(len(sizes))

    query_types = [
        ("point_lookup", "Point Lookups", ax1),
        ("range_query", "Range Queries", ax2),
        ("field_filter", "Field Filters", ax3),
        ("complex_query", "Complex Queries", ax4),
    ]

    for qt, title, ax in query_types:
        memory_data = read_data[f"memory_{qt}"]
        csv_data = read_data[f"csv_{qt}"]

        ax.plot(
            x_pos,
            memory_data,
            "o-",
            label="Memory Storage",
            color="#2E8B57",
            linewidth=2,
            markersize=8,
        )
        ax.plot(
            x_pos,
            csv_data,
            "s-",
            label="CSV Storage",
            color="#CD853F",
            linewidth=2,
            markersize=8,
        )

        ax.set_xlabel("Database Size")
        ax.set_ylabel("Queries per Second")
        ax.set_title(title)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(
            [
                f"{size//1000}K" if size < 1000000 else f"{size//1000000}M"
                for size in sizes
            ]
        )
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.set_yscale("log")

    # Add system info
    system_info = results.get("system_info", {})
    fig.suptitle(
        f"TinyFlux Read Performance Scaling\n{system_info.get('platform', 'Unknown Platform')} - "
        f"Python {system_info.get('python_version', 'Unknown')} - "
        f"TinyFlux {results['test_config']['tinyflux_version']}",
        fontsize=16,
    )

    plt.tight_layout()
    plt.savefig("charts/read_performance_scaling.png", dpi=300, bbox_inches="tight")
    plt.close()
    print("📊 Generated: charts/read_performance_scaling.png")


def plot_performance_comparison(
    write_data: Dict[str, Any],
    read_data: Dict[str, Any],
    results: Dict[str, Any],
):
    """Create performance comparison chart showing key insights."""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

    sizes = write_data["sizes"]
    size_labels = [
        f"{size//1000}K" if size < 1000000 else f"{size//1000000}M"
        for size in sizes
    ]

    # Chart 1: Write Consistency (Memory)
    ax1.bar(
        size_labels, write_data["memory_individual"], color="#2E8B57", alpha=0.8
    )
    ax1.set_title("Memory Storage: Consistent Write Performance")
    ax1.set_ylabel("Individual Writes/sec")
    ax1.grid(True, alpha=0.3)
    avg_perf = np.mean([p for p in write_data["memory_individual"] if p > 0])
    ax1.axhline(
        y=avg_perf,
        color="red",
        linestyle="--",
        label=f"Average: {avg_perf:,.0f} writes/sec",
    )
    ax1.legend()

    # Chart 2: Batch vs Individual Speedup (CSV)
    valid_csv_individual = [p for p in write_data["csv_individual"] if p > 0]
    valid_csv_batch = write_data["csv_batch"][: len(valid_csv_individual)]
    valid_sizes = size_labels[: len(valid_csv_individual)]

    speedup = [b / i for b, i in zip(valid_csv_batch, valid_csv_individual)]

    ax2.bar(valid_sizes, speedup, color="#CD853F", alpha=0.8)
    ax2.set_title("CSV Storage: Batch Insert Speedup")
    ax2.set_ylabel("Speedup Factor (batch/individual)")
    ax2.grid(True, alpha=0.3)
    avg_speedup = np.mean(speedup)
    ax2.axhline(
        y=avg_speedup,
        color="red",
        linestyle="--",
        label=f"Average: {avg_speedup:.1f}x faster",
    )
    ax2.legend()

    # Chart 3: Query Performance Degradation
    point_lookup_memory = read_data["memory_point_lookup"]
    point_lookup_csv = read_data["csv_point_lookup"]

    ax3.plot(
        size_labels,
        point_lookup_memory,
        "o-",
        label="Memory Storage",
        color="#2E8B57",
        linewidth=3,
        markersize=10,
    )
    ax3.plot(
        size_labels,
        point_lookup_csv,
        "s-",
        label="CSV Storage",
        color="#CD853F",
        linewidth=3,
        markersize=10,
    )
    ax3.set_title("Point Lookup Performance vs Database Size")
    ax3.set_ylabel("Queries per Second")
    ax3.set_yscale("log")
    ax3.grid(True, alpha=0.3)
    ax3.legend()

    # Chart 4: Storage Performance Ratio
    ratios = [
        m / c for m, c in zip(point_lookup_memory, point_lookup_csv) if c > 0
    ]
    valid_ratio_sizes = size_labels[: len(ratios)]

    ax4.bar(valid_ratio_sizes, ratios, color="#4682B4", alpha=0.8)
    ax4.set_title("Memory vs CSV Query Speed Advantage")
    ax4.set_ylabel("Memory/CSV Performance Ratio")
    ax4.grid(True, alpha=0.3)
    avg_ratio = np.mean(ratios)
    ax4.axhline(
        y=avg_ratio,
        color="red",
        linestyle="--",
        label=f"Average: {avg_ratio:.0f}x faster",
    )
    ax4.legend()

    # Add system info
    system_info = results.get("system_info", {})
    fig.suptitle(
        f"TinyFlux Performance Analysis\n{system_info.get('platform', 'Unknown Platform')} - "
        f"Python {system_info.get('python_version', 'Unknown')} - "
        f"TinyFlux {results['test_config']['tinyflux_version']}",
        fontsize=16,
    )

    plt.tight_layout()
    plt.savefig("charts/performance_analysis.png", dpi=300, bbox_inches="tight")
    plt.close()
    print("📊 Generated: charts/performance_analysis.png")


def main():
    """Generate all performance visualization charts."""
    print("🎨 TinyFlux Performance Visualization")
    print("=" * 40)
    
    # Create charts directory if it doesn't exist
    os.makedirs("charts", exist_ok=True)

    # Load results
    try:
        results = load_results()
        print(f"✅ Loaded benchmark results from performance_results.json")
    except FileNotFoundError:
        print("❌ performance_results.json not found. Run benchmark.py first.")
        return
    except Exception as e:
        print(f"❌ Error loading results: {e}")
        return

    # Extract data
    write_data = extract_write_performance(results)
    read_data = extract_read_performance(results)

    if not write_data["sizes"]:
        print("❌ No performance data found in results")
        return

    print(
        f"📊 Processing data for {len(write_data['sizes'])} database sizes..."
    )

    # Generate charts
    plot_write_performance_scaling(write_data, results)
    plot_read_performance_scaling(read_data, results)
    plot_performance_comparison(write_data, read_data, results)

    print(f"\n✅ Performance visualization completed!")
    print(f"📁 Generated 3 charts in charts/ directory:")
    print(f"   • charts/write_performance_scaling.png")
    print(f"   • charts/read_performance_scaling.png")
    print(f"   • charts/performance_analysis.png")


if __name__ == "__main__":
    main()
