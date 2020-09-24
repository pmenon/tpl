#include <deque>
#include <memory>
#include <random>
#include <vector>

#include "ips4o.hpp"

#include "benchmark/benchmark.h"
#include "util/chunked_vector.h"
#include "util/sfc_gen.h"

namespace tpl::util {

namespace {

template <typename T>
void FillSequential(T &container, uint32_t size) {
  for (uint32_t i = 0; i < size; i++) {
    container.push_back(i);
  }
}

template <typename T>
void FillRandom(T &container, uint32_t size) {
  util::SFC32 gen;
  std::uniform_int_distribution<uint32_t> dist;
  for (uint32_t i = 0; i < size; i++) {
    container.push_back(dist(gen));
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

}  // namespace

template <typename VectorType>
static void BM_InsertAppend(benchmark::State &state) {
  using ValueType = typename VectorType::value_type;
  for (auto _ : state) {
    VectorType v;
    for (int32_t i = state.range(0); i--;) {
      v.push_back(ValueType(i));
    }
  }
}

template <typename VectorType>
static void BM_Scan(benchmark::State &state) {
  using ValueType = typename VectorType::value_type;

  VectorType v;
  for (int32_t i = state.range(0); i--;) {
    v.push_back(ValueType(i));
  }

  for (auto _ : state) {
    uint32_t c = 0;
    for (auto x : v) {
      c += x;
    }
    benchmark::DoNotOptimize(c);
  }
}

template <typename VectorType>
static void BM_RandomAccess(benchmark::State &state) {
  using ValueType = typename VectorType::value_type;

  VectorType v;
  for (int32_t i = state.range(0); i--;) {
    v.push_back(ValueType(i));
  }

  // Indexes
  std::vector<uint32_t> random_indexes = CreateRandomIndexes(0, state.range(0));

  // Run
  for (auto _ : state) {
    uint32_t c = 0;
    for (auto idx : random_indexes) {
      c += v[idx];
    }
    benchmark::DoNotOptimize(c);
  }
}

template <typename VectorType>
static void BM_Sort_Base(benchmark::State &state) {
  using ValueType = typename VectorType::value_type;
  for (auto _ : state) {
    VectorType v;

    util::SFC32 gen;
    std::uniform_int_distribution<uint32_t> dist;
    for (int32_t i = state.range(0); i--;) {
      v.push_back(ValueType(dist(gen)));
    }
  }
}

template <typename VectorType>
static void BM_Sort(benchmark::State &state) {
  using ValueType = typename VectorType::value_type;
  for (auto _ : state) {
    VectorType v;

    util::SFC32 gen;
    std::uniform_int_distribution<uint32_t> dist;
    for (int32_t i = state.range(0); i--;) {
      v.push_back(ValueType(dist(gen)));
    }

    ips4o::sort(v.begin(), v.end(), std::less<ValueType>{});
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

BENCHMARK_TEMPLATE(BM_InsertAppend, std::vector<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_InsertAppend, std::deque<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_InsertAppend, util::ChunkedVectorT<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Scan, std::vector<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Scan, std::deque<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Scan, util::ChunkedVectorT<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_RandomAccess, std::vector<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_RandomAccess, std::deque<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_RandomAccess, util::ChunkedVectorT<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Sort_Base, std::vector<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Sort, std::vector<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Sort_Base, std::deque<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Sort, std::deque<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Sort_Base, util::ChunkedVectorT<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Sort, util::ChunkedVectorT<uint32_t>)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000)
    ->Unit(benchmark::kMicrosecond);

}  // namespace tpl::util
