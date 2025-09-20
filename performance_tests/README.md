# TinyFlux Performance Tests

This directory contains comprehensive performance benchmarking tools for TinyFlux.

## Quick Start

Install dependencies and run benchmarks:

```bash
pip install -r requirements.txt
python benchmark.py
```

## Generate Performance Charts

Create detailed visualization charts from benchmark results:

```bash
pip install matplotlib numpy  # Additional dependencies for charts
python benchmark.py           # Generate performance_results.json
python visualize_performance.py
```

This generates three professional charts:
- `charts/write_performance_scaling.png` - Write throughput across database sizes
- `charts/read_performance_scaling.png` - Query performance degradation analysis  
- `charts/performance_analysis.png` - Key insights and performance ratios

## System Requirements

**Hardware:**
- ~100MB available RAM for 100K point benchmarks
- Multi-core CPU recommended for faster processing

**Software:**
- Python 3.8+ 
- Dependencies: `psutil`, `matplotlib`, `numpy`

**Time:**
- ~2-3 minutes total benchmark time (varies by system)
- Progress logging shows real-time status and completion estimates

## Benchmark Details

### Write Performance Testing
- **Database sizes**: 10K, 50K, 100K points
- **Individual Inserts**: Single `.insert()` calls (skipped for 100K+ points)
- **Batch Inserts**: `.insert_multiple()` with batch_size=1000
- **Storage types**: Memory vs CSV file storage

### Read Performance Testing  
- **Query types**: Point lookups, range queries, field filters, complex queries
- **Scaling analysis**: Performance degradation with database size
- **Storage comparison**: Memory vs CSV scan performance

### Index Performance Testing
- **Memory usage**: Linear scaling analysis
- **Rebuild time**: Index reconstruction performance
- **Coverage verification**: 100% index coverage validation

## Customization

Modify benchmark parameters in the script:

```python
# Custom benchmark with different sizes
results = run_full_benchmark(
    write_db_sizes=[5000, 25000],      # Custom write test sizes
    query_db_sizes=[10000, 50000]     # Custom query test sizes
)
```

## Output Files

The benchmark generates:
- **Console output**: Real-time progress with timestamps and percentages
- **performance_results.json**: Detailed benchmark data with system info
- **PNG charts**: Professional visualizations in `charts/` directory (when using visualization script)
- **Summary tables**: Key performance metrics and insights
