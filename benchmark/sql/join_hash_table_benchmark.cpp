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

struct CompressedTuple {
  uint64_t k_v1_v2;
};

template <uint64_t N>
struct WrappedTuple {
  sql::HashTableEntry entry;
  Tuple<N> tuple;
};

struct WrappedCompressedTuple {
  sql::HashTableEntry entry;
  CompressedTuple tuple;
};

namespace {

template <typename RNG>
uint32_t bounded_rand(RNG &&rng, uint32_t range) {
  uint32_t x = rng();
  uint64_t m = uint64_t(x) * uint64_t(range);
  uint32_t l = uint32_t(m);
  if (l < range) {
    uint32_t t = -range;
    if (t >= range) {
      t -= range;
      if (t >= range) t %= range;
    }
    while (l < t) {
      x = rng();
      m = uint64_t(x) * uint64_t(range);
      l = uint32_t(m);
    }
  }
  return m >> 32;
}

#if 1
template <uint64_t N>
void PostMaterializationAnalysis(const uint32_t n, const byte **entries,
                                 sql::JoinHashTable::AnalysisStats *stats) {
  uint64_t bits_k = 0, bits_v1 = 0, bits_v2 = 0;
  auto rows = reinterpret_cast<const Tuple<N> **>(entries);
  for (uint32_t i = 0; i < n; i++) {
    bits_k |= rows[i]->key;
    bits_v1 |= rows[i]->vals[0];
    bits_v2 |= rows[i]->vals[1];
  }
  stats->SetNumCols(3);
  stats->SetBitsForCol(0, 64 - util::BitUtil::CountLeadingZeros(bits_k));
  stats->SetBitsForCol(1, 64 - util::BitUtil::CountLeadingZeros(bits_v1));
  stats->SetBitsForCol(2, 64 - util::BitUtil::CountLeadingZeros(bits_v2));
}

template <uint64_t N>
void CompressJoinHashTable(const uint32_t n, const byte *in[], byte *out[]) {
  auto rows = reinterpret_cast<const Tuple<N> **>(in);
  auto compressed = reinterpret_cast<CompressedTuple **>(out);
  for (uint32_t i = 0; i < n; i++) {
    compressed[i]->k_v1_v2 |= rows[i]->key << 32 | rows[i]->vals[0] << 16 | rows[i]->vals[1];
  }
}

#else
void PostMaterializationAnalysis(sql::JoinHashTable::ConstTupleIterator iter,
                                 sql::JoinHashTable::ConstTupleIterator end,
                                 sql::JoinHashTable::AnalysisStats *stats) {
  uint64_t bits_k = 0, bits_v1 = 0, bits_v2 = 0;
  for (; iter != end; ++iter) {
    bits_k |= reinterpret_cast<const WrappedTuple *>(*iter)->tuple.key;
    bits_v1 |= reinterpret_cast<const WrappedTuple *>(*iter)->tuple.val1;
    bits_v2 |= reinterpret_cast<const WrappedTuple *>(*iter)->tuple.val2;
  }
  stats->SetNumCols(3);
  stats->SetBitsForCol(0, 64 - util::BitUtil::CountLeadingZeros(bits_k));
  stats->SetBitsForCol(1, 64 - util::BitUtil::CountLeadingZeros(bits_v1));
  stats->SetBitsForCol(2, 64 - util::BitUtil::CountLeadingZeros(bits_v2));
}

void CompressJoinHashTable(sql::JoinHashTable::ConstTupleIterator in_iter,
                           sql::JoinHashTable::ConstTupleIterator in_end,
                           sql::JoinHashTable::TupleIterator out_iter) {
  for (; in_iter != in_end; ++in_iter, ++out_iter) {
    auto in = reinterpret_cast<const WrappedTuple *>(*in_iter);
    auto out = reinterpret_cast<WrappedCompressedTuple *>(*out_iter);
    out->entry = in->entry;
    out->tuple.k_v1_v2 = in->tuple.key << 32 | in->tuple.val1 << 16 | in->tuple.val2;
  }
}
#endif

template <uint64_t N>
std::unique_ptr<sql::JoinHashTable> CreateAndPopulateJoinHashTable(
    sql::MemoryPool *memory, uint64_t num_tuples, bool concise,
    sql::JoinHashTable::AnalysisPass analysis = nullptr,
    sql::JoinHashTable::CompressPass compress = nullptr) {
  std::unique_ptr<sql::JoinHashTable> jht;

  if (compress) {
    jht =
        std::make_unique<sql::JoinHashTable>(memory, sizeof(Tuple<N>), concise, analysis, compress);
  } else {
    jht = std::make_unique<sql::JoinHashTable>(memory, sizeof(Tuple<N>), concise);
  }

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

template <uint64_t N, bool Concise, bool Compress>
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
    auto jht = Compress ? CreateAndPopulateJoinHashTable<N>(&memory, num_tuples, Concise,
                                                            PostMaterializationAnalysis<N>,
                                                            CompressJoinHashTable<N>)
                        : CreateAndPopulateJoinHashTable<N>(&memory, num_tuples, Concise);
    // Build.
    jht->Build();
    // Probe.
    uint64_t count;
    for (const auto &probe : probe_tuples) {
      count += (jht->template Lookup<Concise>(probe.Hash()) != nullptr);
    }
    benchmark::DoNotOptimize(count);
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

static void CustomArgs(benchmark::internal::Benchmark *b) {
  for (int64_t i = 10; i < 21; i++) {
    b->Arg(1 << i);
  }
}

// ---------------------------------------------------------
// Tuple Size = 16
//BENCHMARK_TEMPLATE(BM_Base, 1)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 1, false, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 1, false, true)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 1, true, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 24
BENCHMARK_TEMPLATE(BM_Base, 2)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 2, false, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 2, false, true)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, 2, true, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 40
//BENCHMARK_TEMPLATE(BM_Base, 4)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 4, false, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 1, false, true)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 4, true, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 72
//BENCHMARK_TEMPLATE(BM_Base, 8)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 8, false, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 1, false, true)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 8, true, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 136
//BENCHMARK_TEMPLATE(BM_Base, 16)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 16, false, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 1, false, true)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
//BENCHMARK_TEMPLATE(BM_Build, 16, true, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);

}  // namespace tpl
