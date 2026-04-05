import random
import time
import gc
import tracemalloc
import statistics
import matplotlib.pyplot as plt

from database.bplustree import BPlusTree
from database.bruteforce import BruteForceDB


class PerformanceAnalyzer:
    # Configure benchmark defaults including tree degree and deterministic randomness.
    def __init__(self, tree_degree=50, seed=42):
        self.tree_degree = tree_degree
        self.rng = random.Random(seed)

    # Measure insertion time for both B+ tree and brute-force data structures.
    def _measure_insert(self, keys):
        bpt = BPlusTree(t=self.tree_degree)
        brute = BruteForceDB()

        start = time.perf_counter()
        for key in keys:
            bpt.insert(key, f"val_{key}")
        bplus_time = time.perf_counter() - start

        start = time.perf_counter()
        for key in keys:
            brute.insert(key)
        brute_time = time.perf_counter() - start

        return bpt, brute, bplus_time, brute_time

    # Measure lookup time for sampled keys in both implementations.
    def _measure_search(self, bpt, brute, key_space, search_count):
        search_keys = self.rng.sample(range(key_space), search_count)

        start = time.perf_counter()
        for key in search_keys:
            bpt.search(key)
        bplus_time = time.perf_counter() - start

        start = time.perf_counter()
        for key in search_keys:
            brute.search(key)
        brute_time = time.perf_counter() - start

        return bplus_time, brute_time

    # Measure range-query performance over randomly generated key intervals.
    def _measure_range_query(self, bpt, brute, key_space, query_count, span):
        ranges = []
        max_start = max(0, key_space - span - 1)
        for _ in range(query_count):
            start = self.rng.randint(0, max_start)
            end = start + span
            ranges.append((start, end))

        start = time.perf_counter()
        for low, high in ranges:
            bpt.range_query(low, high)
        bplus_time = time.perf_counter() - start

        start = time.perf_counter()
        for low, high in ranges:
            brute.range_query(low, high)
        brute_time = time.perf_counter() - start

        return bplus_time, brute_time

    # Measure deletion time for a sampled subset of inserted keys.
    def _measure_delete(self, bpt, brute, keys, delete_count):
        delete_keys = self.rng.sample(keys, delete_count)

        start = time.perf_counter()
        for key in delete_keys:
            bpt.delete(key)
        bplus_time = time.perf_counter() - start

        start = time.perf_counter()
        for key in delete_keys:
            brute.delete(key)
        brute_time = time.perf_counter() - start

        return bplus_time, brute_time

    # Build a B+ tree preloaded with the provided keys.
    def _build_bplus(self, keys):
        bpt = BPlusTree(t=self.tree_degree)
        for key in keys:
            bpt.insert(key, key)
        return bpt

    # Build a brute-force store preloaded with the provided keys.
    def _build_bruteforce(self, keys):
        brute = BruteForceDB()
        for key in keys:
            brute.insert(key)
        return brute

    # Create a deterministic random sequence of insert, search, and delete tasks.
    def _generate_random_workload(self, initial_keys, task_count):
        key_set = set(initial_keys)
        workload = []
        next_key = max(initial_keys) + 1 if initial_keys else 0

        for _ in range(task_count):
            roll = self.rng.random()

            if roll < 0.35:
                key = next_key
                next_key += 1
                key_set.add(key)
                workload.append(("insert", key))
            elif roll < 0.75:
                upper = max(0, next_key - 1)
                key = self.rng.randint(0, upper)
                workload.append(("search", key))
            else:
                if key_set:
                    key = self.rng.choice(tuple(key_set))
                    key_set.remove(key)
                    workload.append(("delete", key))
                else:
                    workload.append(("search", 0))

        return workload

    # Execute a mixed workload on a fresh B+ tree and return elapsed time.
    def _apply_workload_bplus(self, keys, workload):
        bpt = self._build_bplus(keys)
        start = time.perf_counter()
        for op, key in workload:
            if op == "insert":
                bpt.insert(key, key)
            elif op == "search":
                bpt.search(key)
            else:
                bpt.delete(key)
        return time.perf_counter() - start

    # Execute a mixed workload on a fresh brute-force store and return elapsed time.
    def _apply_workload_bruteforce(self, keys, workload):
        brute = self._build_bruteforce(keys)
        start = time.perf_counter()
        for op, key in workload:
            if op == "insert":
                brute.insert(key)
            elif op == "search":
                brute.search(key)
            else:
                brute.delete(key)
        return time.perf_counter() - start

    # Measure total runtime for the same random workload on both structures.
    def _measure_random_performance(self, size, task_count):
        key_space = max(size * 20, task_count * 3)
        initial_keys = self.rng.sample(range(key_space), size)
        workload = self._generate_random_workload(initial_keys, task_count)

        bplus_time = self._apply_workload_bplus(initial_keys, workload)
        brute_time = self._apply_workload_bruteforce(initial_keys, workload)
        return bplus_time, brute_time

    # Measure peak memory consumed while building a structure via a callback.
    def _peak_memory_for_builder(self, builder):
        gc.collect()
        tracemalloc.start()
        try:
            builder()
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        return peak

    # Measure peak memory usage for constructing both data structures.
    def _measure_memory_usage(self, keys):
        bplus_peak = self._peak_memory_for_builder(lambda: self._build_bplus(keys))
        brute_peak = self._peak_memory_for_builder(lambda: self._build_bruteforce(keys))
        return bplus_peak, brute_peak

    # Run repeated combined-operation benchmarks and summarize mean and spread.
    def _measure_automated_benchmark(
        self,
        size,
        benchmark_runs,
        search_count,
        range_query_count,
        delete_count,
    ):
        bplus_totals = []
        brute_totals = []

        for _ in range(benchmark_runs):
            key_space = size * 10
            keys = self.rng.sample(range(key_space), size)

            bpt, brute, bplus_insert, brute_insert = self._measure_insert(keys)

            current_search_count = min(search_count, key_space)
            bplus_search, brute_search = self._measure_search(
                bpt,
                brute,
                key_space,
                current_search_count,
            )

            span = max(5, size // 20)
            bplus_range, brute_range = self._measure_range_query(
                bpt,
                brute,
                key_space,
                range_query_count,
                span,
            )

            current_delete_count = min(delete_count, len(keys))
            bplus_delete, brute_delete = self._measure_delete(
                bpt,
                brute,
                keys,
                current_delete_count,
            )

            bplus_totals.append(bplus_insert + bplus_search + bplus_range + bplus_delete)
            brute_totals.append(brute_insert + brute_search + brute_range + brute_delete)

        return {
            "bplus_mean": statistics.mean(bplus_totals),
            "bplus_std": statistics.pstdev(bplus_totals) if len(bplus_totals) > 1 else 0.0,
            "bruteforce_mean": statistics.mean(brute_totals),
            "bruteforce_std": statistics.pstdev(brute_totals) if len(brute_totals) > 1 else 0.0,
        }

    # Run per-operation timing tests across dataset sizes.
    def run_tests(
        self,
        sizes=None,
        search_count=200,
        range_query_count=80,
        delete_count=200,
    ):
        if sizes is None:
            sizes = list(range(100, 100000, 1000))

        results = {
            "sizes": [],
            "insert": {"bplus": [], "bruteforce": []},
            "search": {"bplus": [], "bruteforce": []},
            "range_query": {"bplus": [], "bruteforce": []},
            "delete": {"bplus": [], "bruteforce": []},
        }

        for size in sizes:
            key_space = size * 10
            keys = self.rng.sample(range(key_space), size)

            bpt, brute, bplus_insert, brute_insert = self._measure_insert(keys)

            current_search_count = min(search_count, key_space)
            bplus_search, brute_search = self._measure_search(
                bpt,
                brute,
                key_space,
                current_search_count,
            )

            span = max(5, size // 20)
            bplus_range, brute_range = self._measure_range_query(
                bpt,
                brute,
                key_space,
                range_query_count,
                span,
            )

            current_delete_count = min(delete_count, len(keys))
            bplus_delete, brute_delete = self._measure_delete(
                bpt,
                brute,
                keys,
                current_delete_count,
            )

            results["sizes"].append(size)
            results["insert"]["bplus"].append(bplus_insert)
            results["insert"]["bruteforce"].append(brute_insert)
            results["search"]["bplus"].append(bplus_search)
            results["search"]["bruteforce"].append(brute_search)
            results["range_query"]["bplus"].append(bplus_range)
            results["range_query"]["bruteforce"].append(brute_range)
            results["delete"]["bplus"].append(bplus_delete)
            results["delete"]["bruteforce"].append(brute_delete)

        return results

    # Run advanced benchmarks for mixed workloads, memory, and repeated suites.
    def run_advanced_tests(
        self,
        sizes=None,
        random_task_count=800,
        benchmark_runs=5,
        search_count=200,
        range_query_count=80,
        delete_count=200,
    ):
        if sizes is None:
            sizes = list(range(10, 100000, 1000))

        results = {
            "sizes": [],
            "random_performance": {"bplus": [], "bruteforce": []},
            "memory_usage": {"bplus": [], "bruteforce": []},
            "automated_benchmark": {
                "bplus_mean": [],
                "bplus_std": [],
                "bruteforce_mean": [],
                "bruteforce_std": [],
            },
        }

        for size in sizes:
            key_space = size * 10
            keys = self.rng.sample(range(key_space), size)

            bplus_random, brute_random = self._measure_random_performance(
                size=size,
                task_count=random_task_count,
            )

            bplus_mem, brute_mem = self._measure_memory_usage(keys)

            suite_stats = self._measure_automated_benchmark(
                size=size,
                benchmark_runs=benchmark_runs,
                search_count=search_count,
                range_query_count=range_query_count,
                delete_count=delete_count,
            )

            results["sizes"].append(size)
            results["random_performance"]["bplus"].append(bplus_random)
            results["random_performance"]["bruteforce"].append(brute_random)
            results["memory_usage"]["bplus"].append(bplus_mem)
            results["memory_usage"]["bruteforce"].append(brute_mem)
            results["automated_benchmark"]["bplus_mean"].append(suite_stats["bplus_mean"])
            results["automated_benchmark"]["bplus_std"].append(suite_stats["bplus_std"])
            results["automated_benchmark"]["bruteforce_mean"].append(suite_stats["bruteforce_mean"])
            results["automated_benchmark"]["bruteforce_std"].append(suite_stats["bruteforce_std"])

        return results

    # Plot basic operation timing curves for B+ tree versus brute force.
    def plot_results(self, results, save_prefix=None, show=True):
        sizes = results["sizes"]
        operations = [
            ("insert", "Insertion Time (s)"),
            ("search", "Search Time (s)"),
            ("range_query", "Range Query Time (s)"),
            ("delete", "Deletion Time (s)"),
        ]

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        axes = axes.ravel()

        for idx, (operation, ylabel) in enumerate(operations):
            axes[idx].plot(sizes, results[operation]["bplus"], label="B+ Tree", marker="o")
            axes[idx].plot(
                sizes,
                results[operation]["bruteforce"],
                label="Brute Force",
                marker="s",
            )
            axes[idx].set_title(operation.replace("_", " ").title())
            axes[idx].set_xlabel("Number of Keys")
            axes[idx].set_ylabel(ylabel)
            axes[idx].grid(True, linestyle="--", alpha=0.4)
            axes[idx].legend()

        fig.tight_layout()

        if save_prefix:
            fig.savefig(f"{save_prefix}_performance.png", dpi=160)

        if show:
            plt.show()

        return fig

    # Plot advanced benchmark charts for random workload, memory, and variance.
    def plot_advanced_results(self, results, save_prefix=None, show=True):
        sizes = results["sizes"]

        fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))

        axes[0].plot(
            sizes,
            results["random_performance"]["bplus"],
            label="B+ Tree",
            marker="o",
        )
        axes[0].plot(
            sizes,
            results["random_performance"]["bruteforce"],
            label="Brute Force",
            marker="s",
        )
        axes[0].set_title("Random Task Mix Time")
        axes[0].set_xlabel("Number of Keys")
        axes[0].set_ylabel("Time (s)")
        axes[0].grid(True, linestyle="--", alpha=0.4)
        axes[0].legend()

        bplus_mem_mb = [value / (1024 * 1024) for value in results["memory_usage"]["bplus"]]
        brute_mem_mb = [value / (1024 * 1024) for value in results["memory_usage"]["bruteforce"]]
        axes[1].plot(sizes, bplus_mem_mb, label="B+ Tree", marker="o")
        axes[1].plot(sizes, brute_mem_mb, label="Brute Force", marker="s")
        axes[1].set_title("Peak Memory Usage")
        axes[1].set_xlabel("Number of Keys")
        axes[1].set_ylabel("Memory (MB)")
        axes[1].grid(True, linestyle="--", alpha=0.4)
        axes[1].legend()

        axes[2].errorbar(
            sizes,
            results["automated_benchmark"]["bplus_mean"],
            yerr=results["automated_benchmark"]["bplus_std"],
            label="B+ Tree",
            marker="o",
            capsize=3,
        )
        axes[2].errorbar(
            sizes,
            results["automated_benchmark"]["bruteforce_mean"],
            yerr=results["automated_benchmark"]["bruteforce_std"],
            label="Brute Force",
            marker="s",
            capsize=3,
        )
        axes[2].set_title("Automated Benchmark (Mean ± Std)")
        axes[2].set_xlabel("Number of Keys")
        axes[2].set_ylabel("Total Time (s)")
        axes[2].grid(True, linestyle="--", alpha=0.4)
        axes[2].legend()

        fig.tight_layout()

        if save_prefix:
            fig.savefig(f"{save_prefix}_advanced_performance.png", dpi=160)

        if show:
            plt.show()

        return fig


if __name__ == "__main__":
    analyzer = PerformanceAnalyzer(tree_degree=50, seed=42)
    quick_sizes = list(range(100, 20100, 2000))
    benchmark_results = analyzer.run_tests(sizes=quick_sizes)
    analyzer.plot_results(benchmark_results, save_prefix="database", show=True)