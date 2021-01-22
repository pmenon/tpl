#include <vector>

#include <random>

#include "benchmark/benchmark.h"
#include "logging/logger.h"
#include "sql/chaining_hash_table.h"
#include "sql/compact_storage.h"
#include "sql/join_hash_table.h"
#include "sql/tuple_id_list.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operations.h"
#include "util/sfc_gen.h"
#include "util/timer.h"

double kAnalyzeMs, kCompressMs;

namespace tpl {

// All keys and values occupy 64 bits.
template <typename T, std::size_t N>
struct Tuple {
  // The key is always 32-bit fat.
  int32_t key;
  // The values are homogenous of type 'T'.
  T vals[N];
  // Construct a tuple with the given key.
  Tuple(int32_t key) : key(key) {}
  // Hash the tuple.
  hash_t Hash() const { return util::HashUtil::Hash(key); }
  // Return the tuple's key.
  auto Key() const { return key; }
  // Check if this tuple is equal to the provided tuple.
  bool operator==(const Tuple<T, N> &that) const { return Key() == that.Key(); }
};

// All keys and values occupy 8 bits.
template <typename T, std::size_t N>
struct CompressedTuple {
  // The individual parts. The key is the first part.
  T parts[N];
  // Return the tuple's key.
  auto Key() const { return parts[0]; }
  // Check if this compressed tuple is equal to the provided FAT tuple.
  template <typename U>
  bool operator==(const Tuple<U, N> &that) const {
    return Key() == static_cast<T>(that.Key());
  }
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
template <typename T, uint64_t N>
void PostMaterializationAnalysis(const uint32_t n, const byte **entries,
                                 sql::JoinHashTable::AnalysisStats *stats) {
  uint64_t bits_k = 0;
  std::array<uint64_t, N> bits_v = {0};
  auto rows = reinterpret_cast<const Tuple<T, N> **>(entries);
  for (uint32_t i = 0; i < n; i++) {
    bits_k |= rows[i]->key;
    // N is templated constexpr, so should be unrolled.
    for (uint32_t j = 0; j < N; j++) bits_v[j] |= rows[i]->vals[j];
  }
  stats->SetNumCols(N + 1);
  stats->SetBitsForCol(0, 64 - util::BitUtil::CountLeadingZeros(bits_k));
  // N is templated constexpr, so should be unrolled.
  for (uint32_t j = 0; j < N; j++) {
    stats->SetBitsForCol(j + 1, 64 - util::BitUtil::CountLeadingZeros(bits_v[j]));
  }
}

template <typename T, typename U, uint64_t N>
void CompressJoinHashTable(const uint32_t n, const byte **RESTRICT in,
                           byte **RESTRICT out) {
  auto rows = reinterpret_cast<const Tuple<T, N> **>(in);
  auto compressed = reinterpret_cast<CompressedTuple<U, N + 1> **>(out);
  for (uint32_t i = 0; i < n; i++) {
    compressed[i]->parts[0] = rows[i]->key;
    // N is templated constexpr, so should be unrolled.
    for (uint32_t j = 0; j < N; j++) {
      compressed[i]->parts[j + 1] = rows[i]->vals[j];
    }
  }
}

#else
template <uint64_t N>
struct WrappedTuple {
  sql::HashTableEntry entry;
  Tuple<N> tuple;
};

struct WrappedCompressedTuple {
  sql::HashTableEntry entry;
  CompressedTuple tuple;
};

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

template <typename T, typename U, uint64_t N>
NEVER_INLINE std::unique_ptr<sql::JoinHashTable> CreateAndPopulateJoinHashTable(
    sql::MemoryPool *memory, uint64_t num_tuples,
    sql::JoinHashTable::AnalysisPass analysis = nullptr,
    sql::JoinHashTable::CompressPass compress = nullptr) {
  std::unique_ptr<sql::JoinHashTable> jht;

  if (compress) {
    jht = std::make_unique<sql::JoinHashTable>(memory, sizeof(Tuple<T, N>), false, analysis,
                                               compress);
  } else {
    jht = std::make_unique<sql::JoinHashTable>(memory, sizeof(Tuple<T, N>));
  }

  util::SFC64 gen(std::random_device{}());
  std::uniform_int_distribution<U> dist(0, std::numeric_limits<U>::max());
  for (uint32_t i = 0; i < num_tuples; i++) {
    Tuple<T, N> tuple(dist(gen));
    for (uint32_t j = 0; j < N; j++) tuple.vals[j] = dist(gen);
    *reinterpret_cast<Tuple<T, N> *>(jht->AllocInputTuple(tuple.Hash())) = tuple;
  }

  return jht;
}

}  // namespace

template <typename T, typename U, uint64_t N, bool Compress>
static void BM_Build(benchmark::State &state) {
  static_assert(sizeof(T) >= sizeof(uint8_t));

  const uint64_t num_tuples = state.range(0);

  // Make build tuples.
  util::SFC64 gen(std::random_device{}());
  std::uniform_int_distribution<T> dist(0, 5000 * std::numeric_limits<U>::max());
  std::vector<Tuple<T, N>> probe_tuples(num_tuples, Tuple<T, N>(0));
  std::ranges::generate(probe_tuples, [&]() { return Tuple<T, N>(dist(gen)); });

  sql::MemoryPool memory(nullptr);
  for (auto _ : state) {
    // Populate.
    auto jht = Compress ? CreateAndPopulateJoinHashTable<T, U, N>(&memory, num_tuples,
                                                                  PostMaterializationAnalysis<T, N>,
                                                                  CompressJoinHashTable<T, U, N>)
                        : CreateAndPopulateJoinHashTable<T, U, N>(&memory, num_tuples);

    util::Timer<std::milli> timer;

    // Build.
    timer.Start();
    jht->Build();
    timer.Stop();
    //    const auto build_ms = timer.GetElapsed();

    // Probe.
    timer.Start();
    using BuildTuple = std::conditional_t<Compress, CompressedTuple<U, N>, Tuple<T, N>>;
    uint64_t count = 0;
    for (const auto &probe : probe_tuples) {
      for (auto e = jht->template Lookup<false>(probe.Hash()); e != nullptr; e = e->next) {
        auto candidate = e->template PayloadAs<BuildTuple>();
        if (*candidate == probe) count++;
      }
    }
    timer.Stop();
#if 0
    const auto probe_ms = timer.GetElapsed();
    if constexpr (Compress) {
      LOG_INFO("Total Compression Time: {:.2f} ms (analyze={:.2f},compress={:.2f})",
               kAnalyzeMs + kCompressMs, kAnalyzeMs, kCompressMs);
    }
    LOG_INFO("Build: {:.2f} ms, Probe: {:.2f} ms", build_ms, probe_ms);
#endif
    //    printf("%lu\n", count);
    benchmark::DoNotOptimize(count);
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

static void CustomArgs(benchmark::internal::Benchmark *b) {
  for (int64_t i = 21; i < 22; i++) {
    b->Arg(1 << i);
  }
}

// clang-format off

// ---------------------------------------------------------
// Tuple Size = 16
BENCHMARK_TEMPLATE(BM_Build, int64_t, int8_t, 1, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, int64_t, int8_t, 1, true)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 24
BENCHMARK_TEMPLATE(BM_Build, int64_t, int8_t, 2, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, int64_t, int8_t, 2, true)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 40
BENCHMARK_TEMPLATE(BM_Build, int64_t, int8_t, 4, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, int64_t, int8_t, 4, true)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 72
BENCHMARK_TEMPLATE(BM_Build, int64_t, int8_t, 8, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, int64_t, int8_t, 8, true)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------
// Tuple Size = 136
BENCHMARK_TEMPLATE(BM_Build, int64_t, int8_t, 16, false)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_Build, int64_t, int8_t, 16, true)->Apply(CustomArgs)->Unit(benchmark::kMillisecond);

// clang-format on

}  // namespace tpl
