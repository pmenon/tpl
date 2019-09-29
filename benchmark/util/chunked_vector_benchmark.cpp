#include <deque>
#include <memory>
#include <random>
#include <vector>

#include "benchmark/benchmark.h"
#include "util/chunked_vector.h"

namespace tpl::util {

class ChunkedVectorBenchmark : public benchmark::Fixture {
 protected:
  template <typename T>
  void FillSequential(T &container, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
      container.push_back(i);
    }
  }

  std::vector<uint32_t> CreateRandomIndexes(uint32_t min, uint32_t max,
                                            uint32_t num_indexes = 10000000) {
    std::default_random_engine generator;
    std::uniform_int_distribution<uint32_t> rng(min, max);
    std::vector<uint32_t> random_indexes(num_indexes);
    for (uint32_t i = 0; i < num_indexes; i++) {
      random_indexes[i] = rng(generator);
    }

    return random_indexes;
  }
};

BENCHMARK_DEFINE_F(ChunkedVectorBenchmark, STL_Vector_InsertAppendBenchmark)
(benchmark::State &state) {
  const std::size_t size = state.range(0);
  for (auto _ : state) {
    std::vector<uint32_t> v;
    FillSequential(v, size);
  }
}

BENCHMARK_DEFINE_F(ChunkedVectorBenchmark, STL_Deque_InsertAppendBenchmark)
(benchmark::State &state) {
  const std::size_t size = state.range(0);
  for (auto _ : state) {
    std::deque<uint32_t> v;
    FillSequential(v, size);
  }
}

BENCHMARK_DEFINE_F(ChunkedVectorBenchmark, ChunkedVector_InsertAppendBenchmark)
(benchmark::State &state) {
  const std::size_t size = state.range(0);
  for (auto _ : state) {
    ChunkedVectorT<uint32_t> v;
    FillSequential(v, size);
  }
}

BENCHMARK_DEFINE_F(ChunkedVectorBenchmark, STL_Vector_ScanBenchmark)
(benchmark::State &state) {
  // Fill
  const std::size_t size = state.range(0);
  std::vector<uint32_t> v;
  FillSequential(v, size);

  for (auto _ : state) {
    uint32_t c = 0;
    for (auto x : v) {
      benchmark::DoNotOptimize(c += x);
    }
    benchmark::ClobberMemory();
  }
}

BENCHMARK_DEFINE_F(ChunkedVectorBenchmark, STL_Deque_ScanBenchmark)
(benchmark::State &state) {
  // Fill
  const std::size_t size = state.range(0);
  std::deque<uint32_t> v;
  FillSequential(v, size);

  for (auto _ : state) {
    uint32_t c = 0;
    for (auto x : v) {
      benchmark::DoNotOptimize(c += x);
    }
    benchmark::ClobberMemory();
  }
}

BENCHMARK_DEFINE_F(ChunkedVectorBenchmark, ChunkedVector_ScanBenchmark)
(benchmark::State &state) {
  // Fill
  const std::size_t size = state.range(0);
  ChunkedVectorT<uint32_t> v;
  FillSequential(v, size);

  for (auto _ : state) {
    uint32_t c = 0;
    for (auto x : v) {
      benchmark::DoNotOptimize(c += x);
    }
    benchmark::ClobberMemory();
  }
}

BENCHMARK_DEFINE_F(ChunkedVectorBenchmark, STL_Vector_RandomAccessBenchmark)
(benchmark::State &state) {
  // Fill
  const std::size_t size = state.range(0);
  std::vector<uint32_t> v;
  FillSequential(v, size);

  // Indexes
  std::vector<uint32_t> random_indexes = CreateRandomIndexes(0, size);

  // Run
  for (auto _ : state) {
    uint32_t c = 0;
    for (auto idx : random_indexes) {
      benchmark::DoNotOptimize(c += v[idx]);
    }
    benchmark::ClobberMemory();
  }
}

BENCHMARK_DEFINE_F(ChunkedVectorBenchmark, STL_Deque_RandomAccessBenchmark)
(benchmark::State &state) {
  // Fill
  const std::size_t size = state.range(0);
  std::deque<uint32_t> v;
  FillSequential(v, size);

  // Indexes
  std::vector<uint32_t> random_indexes = CreateRandomIndexes(0, size);

  for (auto _ : state) {
    uint32_t c = 0;
    for (auto idx : random_indexes) {
      benchmark::DoNotOptimize(c += v[idx]);
    }
    benchmark::ClobberMemory();
  }
}

BENCHMARK_DEFINE_F(ChunkedVectorBenchmark, ChunkedVector_RandomAccessBenchmark)
(benchmark::State &state) {
  // Fill
  const std::size_t size = state.range(0);
  ChunkedVectorT<uint32_t> v;
  FillSequential(v, size);

  // Indexes
  std::vector<uint32_t> random_indexes = CreateRandomIndexes(0, size);

  for (auto _ : state) {
    uint32_t c = 0;
    for (auto idx : random_indexes) {
      benchmark::DoNotOptimize(c += v[idx]);
    }
    benchmark::ClobberMemory();
  }
}

BENCHMARK_REGISTER_F(ChunkedVectorBenchmark, STL_Vector_InsertAppendBenchmark)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(ChunkedVectorBenchmark, STL_Deque_InsertAppendBenchmark)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(ChunkedVectorBenchmark, ChunkedVector_InsertAppendBenchmark)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(ChunkedVectorBenchmark, STL_Vector_ScanBenchmark)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(ChunkedVectorBenchmark, STL_Deque_ScanBenchmark)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(ChunkedVectorBenchmark, ChunkedVector_ScanBenchmark)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(ChunkedVectorBenchmark, STL_Vector_RandomAccessBenchmark)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(ChunkedVectorBenchmark, STL_Deque_RandomAccessBenchmark)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(ChunkedVectorBenchmark, ChunkedVector_RandomAccessBenchmark)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

}  // namespace tpl::util
