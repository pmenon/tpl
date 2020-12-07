#include <vector>

#include <random>

#include "benchmark/benchmark.h"
#include "sql/chaining_hash_table.h"
#include "sql/compact_storage.h"
#include "sql/join_hash_table.h"
#include "sql/tuple_id_list.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operations.h"
#include "util/sfc_gen.h"

namespace tpl {

template <uint64_t N>
struct Tuple {
  uint64_t key;
  uint64_t vals[N];
  Tuple() = default;
  Tuple(uint64_t key) : key(key) {}
  hash_t Hash() const { return util::HashUtil::Hash(key); }
};

namespace {

template <uint64_t N>
std::unique_ptr<sql::JoinHashTable> CreateAndPopulateJoinHashTable(sql::MemoryPool *memory,
                                                                   uint64_t num_tuples,
                                                                   bool concise) {
  std::unique_ptr<sql::JoinHashTable> jht;
  jht = std::make_unique<sql::JoinHashTable>(memory, sizeof(Tuple<N>), concise);

  util::SFC64 gen(std::random_device{}());
  std::uniform_int_distribution<uint64_t> dist(0, num_tuples);
  for (uint32_t i = 0; i < num_tuples; i++) {
    Tuple<N> tuple(dist(gen));
    *reinterpret_cast<Tuple<N> *>(jht->AllocInputTuple(tuple.Hash())) = tuple;
  }

  return jht;
}

}  // namespace

template <uint64_t N>
static void BM_Base(benchmark::State &state) {
  const uint64_t num_tuples = state.range(0);

  sql::MemoryPool memory(nullptr);
  for (auto _ : state) {
    auto jht = CreateAndPopulateJoinHashTable<N>(&memory, num_tuples, false);
  }
}

template <uint64_t N, bool Concise>
static void BM_Build(benchmark::State &state) {
  const uint64_t num_tuples = state.range(0);

  // Make build tuples.
  util::SFC64 gen(std::random_device{}());
  std::uniform_int_distribution<uint64_t> dist(0, num_tuples);
  std::vector<Tuple<N>> probe_tuples(num_tuples);
  std::ranges::generate(probe_tuples, [&]() { return dist(gen); });

  sql::MemoryPool memory(nullptr);
  for (auto _ : state) {
    // Populate.
    auto jht = CreateAndPopulateJoinHashTable<N>(&memory, num_tuples, Concise);
    // Build.
    jht->Build();
    // Probe.
    uint64_t count;
    for (const auto &probe : probe_tuples) {
      jht->template Lookup<Concise>(probe.Hash());
    }
    benchmark::DoNotOptimize(count);
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

static void CustomArguments(benchmark::internal::Benchmark *b) {
  for (int64_t i = 10; i < 21; i++) {
    b->Arg(1 << i);
  }
}

// ---------------------------------------------------------
// Tuple Size = 16
BENCHMARK_TEMPLATE(BM_Base, 1)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 1, false)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 1, true)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 24
BENCHMARK_TEMPLATE(BM_Base, 2)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 2, false)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 2, true)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 40
BENCHMARK_TEMPLATE(BM_Base, 4)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 4, false)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 4, true)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 72
BENCHMARK_TEMPLATE(BM_Base, 8)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 8, false)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 8, true)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 136
BENCHMARK_TEMPLATE(BM_Base, 16)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 16, false)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 16, true)->Apply(CustomArguments)->Unit(benchmark::kMillisecond);

}  // namespace tpl
