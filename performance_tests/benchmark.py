#!/usr/bin/env python3
"""
TinyFlux Performance Benchmark Suite.

Comprehensive performance testing for TinyFlux database operations including
write throughput, query performance, and storage comparison.
"""
import time
import tempfile
import os
import platform
import psutil
import random
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from tinyflux import TinyFlux, Point
from tinyflux.storages import MemoryStorage, CSVStorage
from tinyflux.queries import FieldQuery, TagQuery, TimeQuery, MeasurementQuery
from tinyflux.version import __version__


def get_system_info() -> Dict[str, str]:
    """Get system information for benchmark context."""
    return {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "cpu_count": str(psutil.cpu_count(logical=False)),
        "cpu_count_logical": str(psutil.cpu_count(logical=True)),
        "memory_gb": f"{psutil.virtual_memory().total / (1024**3):.1f}",
        "architecture": platform.architecture()[0],
    }


def generate_time_series_data(count: int, start_time: datetime) -> List[Point]:
    """Generate realistic time series data for benchmarking."""
    points = []
    current_time = start_time

    # Simulate multiple sensors/devices
    sensors = [f"sensor_{i:03d}" for i in range(20)]
    locations = ["datacenter_1", "datacenter_2", "edge_device", "mobile_unit"]
    measurements = ["temperature", "cpu_usage", "memory_usage", "network_io"]

    for i in range(count):
        # Increment time by 1-5 seconds (realistic IoT frequency)
        current_time += timedelta(seconds=random.randint(1, 5))

        point = Point(
            time=current_time,
            measurement=random.choice(measurements),
            tags={
                "sensor_id": random.choice(sensors),
                "location": random.choice(locations),
                "device_type": random.choice(
                    ["server", "raspberry_pi", "arduino"]
                ),
            },
            fields={
                "value": random.uniform(0.0, 100.0),
                "status": random.choice([1, 0]),
                "batch_id": i // 1000,  # Group into batches
            },
        )
        points.append(point)

    return points


def benchmark_individual_writes(
    storage_type: str, points: List[Point]
) -> Dict[str, Any]:
    """Benchmark individual .insert() performance."""
    print(
        f"\n📝 Individual Insert Benchmark - {storage_type} Storage ({len(points):,} points)"
    )

    if storage_type.lower() == "memory":
        db = TinyFlux(storage=MemoryStorage)
        csv_path = None
    else:  # CSV
        temp_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        csv_path = temp_file.name
        temp_file.close()
        db = TinyFlux(csv_path, storage=CSVStorage)

    try:
        # Warm-up
        warm_up_points = points[:10]
        for point in warm_up_points:
            db.insert(point)

        # Actual benchmark - individual inserts
        start_time = time.time()
        count = 0
        for point in points:
            db.insert(point)
            count += 1
        end_time = time.time()

        total_time = end_time - start_time
        writes_per_second = count / total_time

        # Get file size for CSV storage
        file_size_mb = 0
        if storage_type.lower() == "csv":
            file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)

        result = {
            "storage_type": storage_type,
            "method": "individual_insert",
            "total_points": count,
            "total_time_seconds": round(total_time, 3),
            "writes_per_second": int(writes_per_second),
            "batch_size": 1,
            "file_size_mb": (
                round(file_size_mb, 2) if file_size_mb > 0 else "N/A"
            ),
        }

        print(
            f"  ✅ {writes_per_second:,.0f} writes/sec ({total_time:.2f}s total)"
        )
        if file_size_mb > 0:
            print(f"  📁 File size: {file_size_mb:.1f} MB")

        return result, db

    finally:
        # Clean up CSV file
        if csv_path and os.path.exists(csv_path):
            try:
                os.unlink(csv_path)
            except:
                pass


def benchmark_batch_writes(
    storage_type: str, points: List[Point], batch_size: int = 1000
) -> Dict[str, Any]:
    """Benchmark .insert_multiple() performance."""
    print(
        f"\n📝 Batch Insert Benchmark - {storage_type} Storage ({len(points):,} points, batch_size={batch_size})"
    )

    if storage_type.lower() == "memory":
        db = TinyFlux(storage=MemoryStorage)
        csv_path = None
    else:  # CSV
        temp_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        csv_path = temp_file.name
        temp_file.close()
        db = TinyFlux(csv_path, storage=CSVStorage)

    try:
        # Warm-up
        warm_up_points = points[:100]
        db.insert_multiple(warm_up_points)

        # Actual benchmark
        start_time = time.time()
        count = db.insert_multiple(points, batch_size=batch_size)
        end_time = time.time()

        total_time = end_time - start_time
        writes_per_second = count / total_time

        # Get file size for CSV storage
        file_size_mb = 0
        if storage_type.lower() == "csv":
            file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)

        result = {
            "storage_type": storage_type,
            "method": "insert_multiple",
            "total_points": count,
            "total_time_seconds": round(total_time, 3),
            "writes_per_second": int(writes_per_second),
            "batch_size": batch_size,
            "file_size_mb": (
                round(file_size_mb, 2) if file_size_mb > 0 else "N/A"
            ),
        }

        print(
            f"  ✅ {writes_per_second:,.0f} writes/sec ({total_time:.2f}s total)"
        )
        if file_size_mb > 0:
            print(f"  📁 File size: {file_size_mb:.1f} MB")

        return result, db

    finally:
        # Clean up CSV file
        if csv_path and os.path.exists(csv_path):
            try:
                os.unlink(csv_path)
            except:
                pass


def benchmark_writes_at_scale(
    storage_type: str, db_sizes: List[int], current_test: int, total_tests: int
) -> Dict[str, Dict[str, Any]]:
    """Benchmark write performance at different database sizes."""
    print(f"\n📝 Write Performance Scaling - {storage_type} Storage")

    results = {}

    for i, db_size in enumerate(db_sizes):
        current_test += 1
        progress = (current_test / total_tests) * 100
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(
            f"\n  [{timestamp}] ({progress:.0f}%) Testing writes with {db_size:,} points..."
        )

        # Generate test data
        start_time = datetime.now(timezone.utc) - timedelta(days=7)
        test_points = generate_time_series_data(db_size, start_time)

        size_results = {}

        # Test individual inserts (only for smaller databases to avoid long waits)
        if db_size <= 100000:
            individual_result, _ = benchmark_individual_writes(
                storage_type, test_points
            )
            size_results["individual"] = individual_result
            current_test += 1
        else:
            print(
                f"    ⏭️  Skipping individual inserts for {db_size:,} points (would take too long)"
            )
            size_results["individual"] = {
                "storage_type": storage_type,
                "method": "individual_insert",
                "total_points": db_size,
                "writes_per_second": "N/A (skipped)",
                "note": "Skipped for performance - would take several minutes",
            }
            current_test += 1

        # Test batch inserts (always run)
        batch_result, _ = benchmark_batch_writes(
            storage_type, test_points, batch_size=1000
        )
        size_results["batch"] = batch_result
        current_test += 1

        results[f"{db_size:,}"] = size_results

    return results


def benchmark_reads_at_scale(
    storage_type: str, db_sizes: List[int]
) -> Dict[str, Dict[str, Any]]:
    """Benchmark read performance at different database sizes."""
    print(f"\n📖 Read Performance Scaling - {storage_type} Storage")

    results = {}

    for db_size in db_sizes:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"\n  [{timestamp}] Testing queries with {db_size:,} points...")

        # Create database with specific size
        if storage_type.lower() == "memory":
            db = TinyFlux(storage=MemoryStorage)
            csv_path = None
        else:  # CSV
            temp_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False
            )
            csv_path = temp_file.name
            temp_file.close()
            db = TinyFlux(csv_path, storage=CSVStorage)

        try:
            # Generate and insert test data
            start_time = datetime.now(timezone.utc) - timedelta(days=7)
            test_points = generate_time_series_data(db_size, start_time)
            db.insert_multiple(test_points, batch_size=5000)

            # Get sample data for realistic queries
            sample_points = db.all()[:50]
            measurements = list(set(p.measurement for p in sample_points))
            sensor_ids = list(
                set(p.tags.get("sensor_id", "") for p in sample_points)
            )

            # Define query types
            now = datetime.now(timezone.utc)
            hour_ago = now - timedelta(hours=1)

            query_types = {
                "point_lookup": lambda: db.search(
                    TagQuery().sensor_id == random.choice(sensor_ids)
                ),
                "range_query": lambda: db.search(
                    (TimeQuery() >= hour_ago) & (TimeQuery() <= now)
                ),
                "field_filter": lambda: db.search(FieldQuery().value >= 50),
                "complex_query": lambda: db.search(
                    (FieldQuery().value >= 25)
                    & (FieldQuery().value <= 75)
                    & (TagQuery().location == "datacenter_1")
                ),
            }

            size_results = {}

            for query_name, query_func in query_types.items():
                # Warm-up
                for _ in range(3):
                    query_func()

                # Single query timing (since we want minimal queries)
                start_time = time.time()
                results_list = query_func()
                end_time = time.time()

                query_time_ms = (end_time - start_time) * 1000
                qps = (
                    1000 / query_time_ms if query_time_ms > 0 else float("inf")
                )

                size_results[query_name] = {
                    "time_ms": round(query_time_ms, 1),
                    "qps": int(qps) if qps != float("inf") else 999999,
                    "results_count": len(results_list),
                }

                print(
                    f"    🔍 {query_name}: {qps:.0f} qps ({query_time_ms:.1f}ms)"
                )

            results[f"{db_size:,}"] = size_results

        finally:
            # Clean up CSV file
            if csv_path and os.path.exists(csv_path):
                try:
                    os.unlink(csv_path)
                except:
                    pass

    return results


def benchmark_index_operations(
    db: TinyFlux, storage_type: str
) -> Dict[str, Any]:
    """Benchmark index building performance and memory usage."""
    print(f"\n🗃️  Index Benchmark - {storage_type} Storage")

    # Invalidate index to force rebuild
    db._index.invalidate()

    # Measure index rebuild time
    start_time = time.time()
    db.reindex()
    end_time = time.time()

    rebuild_time = end_time - start_time

    # Measure index memory usage
    def get_deep_size(obj, seen=None):
        """Calculate deep memory usage of an object."""
        size = sys.getsizeof(obj)
        if seen is None:
            seen = set()

        obj_id = id(obj)
        if obj_id in seen:
            return 0

        seen.add(obj_id)

        if isinstance(obj, dict):
            size += sum([get_deep_size(v, seen) for v in obj.values()])
            size += sum([get_deep_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, "__dict__"):
            size += get_deep_size(obj.__dict__, seen)
        elif hasattr(obj, "__iter__") and not isinstance(
            obj, (str, bytes, bytearray)
        ):
            try:
                size += sum([get_deep_size(i, seen) for i in obj])
            except TypeError:
                pass

        return size

    index_memory_bytes = get_deep_size(db._index)
    index_memory_mb = index_memory_bytes / (1024 * 1024)

    # Get index statistics
    index_size = len(db._index)
    total_points = len(db)

    result = {
        "rebuild_time_seconds": round(rebuild_time, 3),
        "index_memory_mb": round(index_memory_mb, 2),
        "index_memory_bytes": index_memory_bytes,
        "index_size": index_size,
        "total_points": total_points,
        "index_efficiency": (
            round((index_size / total_points * 100), 1)
            if total_points > 0
            else 0
        ),
    }

    print(f"  🔄 Index rebuild: {rebuild_time:.3f}s")
    print(
        f"  💾 Index memory: {index_memory_mb:.1f} MB ({index_memory_bytes:,} bytes)"
    )
    print(
        f"  📊 Index size: {index_size:,} points ({result['index_efficiency']:.1f}% coverage)"
    )

    return result


def run_full_benchmark(
    write_db_sizes: List[int] = None, query_db_sizes: List[int] = None
) -> Dict[str, Any]:
    """Run complete benchmark suite."""
    print("🚀 TinyFlux Performance Benchmark Suite")
    print("=" * 50)

    # System information
    system_info = get_system_info()
    print(f"💻 System: {system_info['platform']}")
    print(
        f"🔧 CPU: {system_info['processor']} ({system_info['cpu_count']} cores)"
    )
    print(f"💾 Memory: {system_info['memory_gb']} GB")
    print(f"🐍 Python: {system_info['python_version']}")

    # Default database sizes for testing
    if write_db_sizes is None:
        write_db_sizes = [10000, 50000, 100000]
    if query_db_sizes is None:
        query_db_sizes = [10000, 50000, 100000]

    print(f"📋 Test Plan:")
    print(
        f"  • Write performance at: {', '.join(f'{size:,}' for size in write_db_sizes)} points"
    )
    print(
        f"  • Query performance at: {', '.join(f'{size:,}' for size in query_db_sizes)} points"
    )
    print(f"  • Storage types: Memory, CSV File")

    benchmark_results = {
        "system_info": system_info,
        "test_config": {
            "write_db_sizes": write_db_sizes,
            "query_db_sizes": query_db_sizes,
            "test_date": datetime.now().isoformat(),
            "tinyflux_version": __version__,
        },
        "write_performance_scaling": {},
        "read_performance_scaling": {},
        "index_performance": {},
    }

    total_tests = (
        len(write_db_sizes) * 2 * 2 + len(query_db_sizes) * 2
    )  # write(individual+batch) * 2 storages + query * 2 storages
    current_test = 0

    # Write performance scaling tests
    print(f"\n🔄 PHASE 1: Write Performance Scaling")
    for storage_type in ["Memory", "CSV"]:
        write_scaling_results = benchmark_writes_at_scale(
            storage_type, write_db_sizes, current_test, total_tests
        )
        benchmark_results["write_performance_scaling"][
            storage_type.lower()
        ] = write_scaling_results
        current_test += (
            len(write_db_sizes) * 2
        )  # individual + batch for each size

    # Read performance scaling tests
    print(f"\n🔄 PHASE 2: Read Performance Scaling")
    for storage_type in ["Memory", "CSV"]:
        read_scaling_results = benchmark_reads_at_scale(
            storage_type, query_db_sizes
        )
        benchmark_results["read_performance_scaling"][
            storage_type.lower()
        ] = read_scaling_results
        current_test += len(query_db_sizes)

    # Index performance (using largest database)
    print(f"\n🔄 PHASE 3: Index Performance")
    largest_db_size = max(write_db_sizes)
    for storage_type in ["Memory", "CSV"]:
        print(
            f"\n🗃️  Building {storage_type} database with {largest_db_size:,} points for index testing..."
        )

        if storage_type.lower() == "memory":
            db = TinyFlux(storage=MemoryStorage)
        else:
            temp_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False
            )
            csv_path = temp_file.name
            temp_file.close()
            db = TinyFlux(csv_path, storage=CSVStorage)

        # Generate and load data quickly with insert_multiple
        start_time = datetime.now(timezone.utc) - timedelta(days=7)
        test_points = generate_time_series_data(largest_db_size, start_time)
        db.insert_multiple(test_points, batch_size=1000)

        index_result = benchmark_index_operations(db, storage_type)
        benchmark_results["index_performance"][
            storage_type.lower()
        ] = index_result

        # Clean up CSV file
        if storage_type.lower() == "csv":
            try:
                os.unlink(csv_path)
            except:
                pass

    return benchmark_results


def print_summary_table(results: Dict[str, Any]):
    """Print formatted benchmark results table."""
    print(f"\n📋 PERFORMANCE SUMMARY")
    print("=" * 80)

    # Write Performance Scaling Table
    print(f"\n✍️  WRITE PERFORMANCE SCALING")
    print("-" * 90)

    if "write_performance_scaling" in results:
        # Get database sizes from results
        sample_storage = list(results["write_performance_scaling"].keys())[0]
        db_sizes = list(
            results["write_performance_scaling"][sample_storage].keys()
        )

        for storage, scaling_data in results[
            "write_performance_scaling"
        ].items():
            print(f"\n{storage.upper()} STORAGE:")
            print(
                f"{'Database Size':<15} {'Individual (ops/s)':<18} {'Batch (ops/s)':<15} {'File Size':<12}"
            )
            print("-" * 60)

            for size in db_sizes:
                if size in scaling_data:
                    individual = scaling_data[size]["individual"]
                    batch = scaling_data[size]["batch"]

                    individual_rate = (
                        f"{individual['writes_per_second']:,}"
                        if isinstance(individual["writes_per_second"], int)
                        else str(individual["writes_per_second"])
                    )
                    batch_rate = f"{batch['writes_per_second']:,}"
                    file_size = (
                        f"{batch['file_size_mb']} MB"
                        if batch["file_size_mb"] != "N/A"
                        else "N/A"
                    )

                    print(
                        f"{size:<15} {individual_rate:<18} {batch_rate:<15} {file_size:<12}"
                    )
                else:
                    print(f"{size:<15} {'N/A':<18} {'N/A':<15} {'N/A':<12}")
            print()

    # Read Performance Scaling Table
    print(
        f"\n🔍 READ PERFORMANCE SCALING (Queries per Second by Database Size)"
    )
    print("-" * 80)

    # Get database sizes from results
    if "read_performance_scaling" in results:
        sample_storage = list(results["read_performance_scaling"].keys())[0]
        db_sizes = list(
            results["read_performance_scaling"][sample_storage].keys()
        )
        query_types = [
            "point_lookup",
            "range_query",
            "field_filter",
            "complex_query",
        ]

        for storage, scaling_data in results[
            "read_performance_scaling"
        ].items():
            print(f"\n{storage.upper()} STORAGE:")
            print(f"{'Query Type':<15}", end="")
            for size in db_sizes:
                print(f"{size:<12}", end="")
            print()
            print("-" * (15 + len(db_sizes) * 12))

            for qt in query_types:
                print(f"{qt.replace('_', ' ').title():<15}", end="")
                for size in db_sizes:
                    if size in scaling_data and qt in scaling_data[size]:
                        qps = scaling_data[size][qt]["qps"]
                        print(f"{qps:,}".ljust(12), end="")
                    else:
                        print("N/A".ljust(12), end="")
                print()

    # Index Performance Table
    print(f"\n🗃️  INDEX PERFORMANCE")
    print("-" * 70)
    print(
        f"{'Storage':<12} {'Rebuild (s)':<12} {'Memory (MB)':<12} {'Coverage (%)':<12}"
    )
    print("-" * 70)

    for storage, data in results["index_performance"].items():
        storage_name = storage.title()
        rebuild_time = f"{data['rebuild_time_seconds']}"
        memory_mb = f"{data['index_memory_mb']}"
        coverage = f"{data['index_efficiency']}%"
        print(
            f"{storage_name:<12} {rebuild_time:<12} {memory_mb:<12} {coverage:<12}"
        )


def main():
    """Main benchmark execution."""
    try:
        results = run_full_benchmark()
        print_summary_table(results)

        print(f"\n✅ Benchmark completed successfully!")

        # Find peak performances from the scaling results
        peak_individual = 0
        peak_batch = 0

        if "write_performance_scaling" in results:
            for storage_data in results["write_performance_scaling"].values():
                for size_data in storage_data.values():
                    # Individual performance (skip if it was skipped for large DBs)
                    if "individual" in size_data:
                        individual_perf = size_data["individual"][
                            "writes_per_second"
                        ]
                        if (
                            isinstance(individual_perf, int)
                            and individual_perf > peak_individual
                        ):
                            peak_individual = individual_perf

                    # Batch performance
                    if "batch" in size_data:
                        batch_perf = size_data["batch"]["writes_per_second"]
                        if (
                            isinstance(batch_perf, int)
                            and batch_perf > peak_batch
                        ):
                            peak_batch = batch_perf

        if peak_individual > 0:
            print(
                f"🏆 Peak individual insert performance: {peak_individual:,} writes/sec"
            )
        if peak_batch > 0:
            print(
                f"🚀 Peak batch insert performance: {peak_batch:,} writes/sec"
            )

        # Save detailed results
        import json

        with open("performance_results.json", "w") as f:
            json.dump(results, f, indent=2)
        print(f"📄 Detailed results saved to: performance_results.json")

    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    main()
