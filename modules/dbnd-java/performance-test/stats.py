import statistics


def print_stats(bench_times):
    print(bench_times)
    if len(bench_times) > 1:
        print(f"Min: {min(bench_times)}")
        print(f"Max: {max(bench_times)}")
        print(f"Mean: {statistics.mean(bench_times)}")
        print(f"Standard deviation: {statistics.stdev(bench_times)}")
