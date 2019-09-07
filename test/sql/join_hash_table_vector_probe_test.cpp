#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "sql/join_hash_table.h"
#include "sql/join_hash_table_vector_probe.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/hash.h"

namespace tpl::sql {

/// This is the tuple we insert into the hash table
template <uint8_t N>
struct Tuple {
  int32_t build_key;
  uint32_t aux[N];

  auto Hash() { return util::Hasher::Hash(build_key); }
};

class JoinHashTableVectorProbeTest : public TplTest {
 public:
  JoinHashTableVectorProbeTest() : memory_(nullptr) {}

  MemoryPool *memory() { return &memory_; }

 protected:
  template <uint8_t N, typename F>
  std::unique_ptr<const JoinHashTable> BuildJoinHashTable(bool concise, uint32_t num_tuples,
                                                          F &&key_gen) {
    auto jht = std::make_unique<JoinHashTable>(memory(), sizeof(Tuple<N>), concise);

    // Insert
    for (uint32_t i = 0; i < num_tuples; i++) {
      auto key = key_gen();
      auto hash = util::Hasher::Hash(key);
      auto *tuple = reinterpret_cast<Tuple<N> *>(jht->AllocInputTuple(hash));
      tuple->build_key = key;
    }

    // Build
    jht->Build();

    // Finish
    return jht;
  }

 private:
  MemoryPool memory_;
};

template <uint8_t N>
static hash_t HashTupleInVPI(VectorProjectionIterator *vpi) noexcept {
  const auto *key_ptr = vpi->GetValue<int32_t, false>(0, nullptr);
  return util::Hasher::Hash(*key_ptr);
}

/// The function to determine whether two tuples have equivalent keys
template <uint8_t N>
static bool CmpTupleInVPI(const void *table_tuple, VectorProjectionIterator *vpi) noexcept {
  auto lhs_key = reinterpret_cast<const Tuple<N> *>(table_tuple)->build_key;
  auto rhs_key = *vpi->GetValue<int32_t, false>(0, nullptr);
  return lhs_key == rhs_key;
}

// Sequential number functor
struct Seq {
  int32_t c;
  explicit Seq(uint32_t cc) : c(cc) {}
  int32_t operator()() noexcept { return c++; }
};

struct Range {
  std::random_device random;
  std::uniform_int_distribution<uint32_t> dist;
  Range(int32_t min, int32_t max) : dist(min, max) {}
  int32_t operator()() noexcept { return dist(random); }
};

// Random number functor
struct Rand {
  std::random_device random;
  Rand() = default;
  int32_t operator()() noexcept { return random(); }
};

TEST_F(JoinHashTableVectorProbeTest, SimpleGenericLookupTest) {
  constexpr const uint8_t N = 1;
  constexpr const uint32_t num_build = 1000;
  constexpr const uint32_t num_probe = num_build * 10;

  // Create test JHT
  auto jht = BuildJoinHashTable<N>(/*concise*/ false, num_build, Seq(0));

  // Create test probe input
  auto probe_keys = std::vector<uint32_t>(num_probe);
  std::generate(probe_keys.begin(), probe_keys.end(), Range(0, num_build - 1));

  Schema schema({{"probeKey", IntegerType::InstanceNonNullable()}});

  VectorProjection vp;
  vp.InitializeEmpty({schema.GetColumnInfo(0)});

  VectorProjectionIterator vpi(&vp);

  // Lookup
  JoinHashTableVectorProbe lookup(*jht);

  uint32_t count = 0;
  for (uint32_t i = 0; i < num_probe; i += kDefaultVectorSize) {
    uint32_t size = std::min(kDefaultVectorSize, num_probe - i);

    // Setup VP
    vp.ResetColumn(reinterpret_cast<byte *>(&probe_keys[i]), nullptr, 0, size);
    vpi.SetVectorProjection(&vp);

    // Lookup
    lookup.Prepare(&vpi, HashTupleInVPI<N>);

    // Iterate all
    while (auto *tuple = lookup.GetNextOutput<Tuple<N>>(&vpi, CmpTupleInVPI<N>)) {
      count++;
      auto probe_key = *vpi.GetValue<int32_t, false>(0, nullptr);
      EXPECT_EQ(tuple->build_key, probe_key);
    }
  }

  EXPECT_EQ(num_probe, count);
}

TEST_F(JoinHashTableVectorProbeTest, DISABLED_PerfLookupTest) {
  auto bench = [this](bool concise) {
    constexpr const uint8_t N = 1;
    constexpr const uint32_t num_build = 5000000;
    constexpr const uint32_t num_probe = num_build * 10;

    // Create test JHT
    auto jht = BuildJoinHashTable<N>(concise, num_build, Seq(0));

    // Create test probe input
    auto probe_keys = std::vector<uint32_t>(num_probe);
    std::generate(probe_keys.begin(), probe_keys.end(), Range(0, num_build - 1));

    Schema schema({{"probeKey", IntegerType::InstanceNonNullable()}});

    VectorProjection vp;
    vp.InitializeEmpty({schema.GetColumnInfo(0)});

    VectorProjectionIterator vpi(&vp);

    // Lookup
    JoinHashTableVectorProbe lookup(*jht);

    util::Timer<std::milli> timer;
    timer.Start();

    // Loop over all matches
    uint32_t count = 0;
    for (uint32_t i = 0; i < num_probe; i += kDefaultVectorSize) {
      uint32_t size = std::min(kDefaultVectorSize, num_probe - i);

      // Setup VP
      vp.ResetColumn(reinterpret_cast<byte *>(&probe_keys[i]), nullptr, 0, size);
      vpi.SetVectorProjection(&vp);

      // Lookup
      lookup.Prepare(&vpi, HashTupleInVPI<N>);

      // Iterate all
      while (const auto *entry = lookup.GetNextOutput(&vpi, CmpTupleInVPI<N>)) {
        (void)entry;
        count++;
      }
    }

    timer.Stop();
    auto mtps = (num_probe / timer.elapsed()) / 1000.0;
    LOG_INFO("========== {} ==========", concise ? "Concise" : "Generic");
    LOG_INFO("# Probes    : {}", num_probe)
    LOG_INFO("Probe Time  : {} ms ({:.2f} Mtps)", timer.elapsed(), mtps);
  };

  bench(false);
  bench(true);
}

}  // namespace tpl::sql
