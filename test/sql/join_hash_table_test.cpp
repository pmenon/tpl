#include <random>
#include <vector>

#include "sql/join_hash_table.h"
#include "sql/thread_state_container.h"
#include "util/hash_util.h"
#include "util/test_harness.h"

namespace tpl::sql {

/// This is the tuple we insert into the hash table
struct Tuple {
  uint64_t a, b, c, d;

  hash_t Hash() const { return util::HashUtil::HashMurmur(a); }
};

class JoinHashTableTest : public TplTest {
 public:
  JoinHashTableTest() : memory_(nullptr) {}

  MemoryPool *memory() { return &memory_; }

 private:
  MemoryPool memory_;
};

TEST_F(JoinHashTableTest, LazyInsertionTest) {
  // Test data
  const uint32_t num_tuples = 10;
  std::vector<Tuple> tuples(num_tuples);

  // Populate test data
  {
    std::mt19937 generator;
    std::uniform_int_distribution<uint64_t> distribution;

    for (uint32_t i = 0; i < num_tuples; i++) {
      tuples[i].a = distribution(generator);
      tuples[i].b = distribution(generator);
      tuples[i].c = distribution(generator);
      tuples[i].d = distribution(generator);
    }
  }

  JoinHashTable join_hash_table(memory(), sizeof(Tuple));

  // The table
  for (const auto &tuple : tuples) {
    // Allocate
    auto *space = join_hash_table.AllocInputTuple(tuple.Hash());
    // Insert (by copying) into table
    *reinterpret_cast<Tuple *>(space) = tuple;
  }

  // Before build, the generic hash table shouldn't be populated, but the join
  // table's storage should have buffered all input tuples
  EXPECT_EQ(num_tuples, join_hash_table.GetTupleCount());
  EXPECT_EQ(0u, join_hash_table.chaining_hash_table_.GetElementCount());

  // Try to build
  join_hash_table.Build();

  // Post-build, the sizes should be synced up since all tuples were inserted
  // into the GHT
  EXPECT_EQ(num_tuples, join_hash_table.GetTupleCount());
  EXPECT_EQ(num_tuples, join_hash_table.chaining_hash_table_.GetElementCount());
}

void PopulateJoinHashTable(JoinHashTable *jht, uint32_t num_tuples, uint32_t dup_scale_factor) {
  for (uint32_t rep = 0; rep < dup_scale_factor; rep++) {
    for (uint32_t i = 0; i < num_tuples; i++) {
      // Create tuple
      auto tuple = Tuple{i, 1, 2};
      // Allocate in hash table
      auto *space = jht->AllocInputTuple(tuple.Hash());
      // Copy contents into hash
      *reinterpret_cast<Tuple *>(space) = tuple;
    }
  }
}

template <bool UseCHT>
void BuildAndProbeTest(uint32_t num_tuples, uint32_t dup_scale_factor) {
  // The join table.
  MemoryPool memory(nullptr);
  JoinHashTable join_hash_table(&memory, sizeof(Tuple), UseCHT);

  // Populate.
  PopulateJoinHashTable(&join_hash_table, num_tuples, dup_scale_factor);

  // Build.
  join_hash_table.Build();

  // Do some successful lookups.
  for (uint32_t i = 0; i < num_tuples; i++) {
    // The probe tuple
    Tuple probe_tuple = {i, 0, 0, 0};
    // Perform probe
    uint32_t count = 0;
    for (auto iter = join_hash_table.Lookup<UseCHT>(probe_tuple.Hash()); iter.HasNext();) {
      auto *matched = reinterpret_cast<const Tuple *>(iter.GetMatchPayload());
      if (matched->a == probe_tuple.a) {
        count++;
      }
    }
    EXPECT_EQ(dup_scale_factor, count)
        << "Expected to find " << dup_scale_factor << " matches, but key [" << i << "] found "
        << count << " matches";
  }

  // Do some unsuccessful lookups.
  for (uint32_t i = num_tuples; i < num_tuples + 1000; i++) {
    // A tuple that should NOT find any join partners
    Tuple probe_tuple = {i, 0, 0, 0};
    for (auto iter = join_hash_table.Lookup<UseCHT>(probe_tuple.Hash()); iter.HasNext();) {
      FAIL() << "Should not find any matches for key [" << i
             << "] that was not inserted into the join hash table";
    }
  }
}

TEST_F(JoinHashTableTest, UniqueKeyLookupTest) { BuildAndProbeTest<false>(400, 1); }

TEST_F(JoinHashTableTest, DuplicateKeyLookupTest) { BuildAndProbeTest<false>(400, 5); }

TEST_F(JoinHashTableTest, UniqueKeyConciseTableTest) { BuildAndProbeTest<true>(400, 1); }

TEST_F(JoinHashTableTest, DuplicateKeyLookupConciseTableTest) { BuildAndProbeTest<true>(400, 5); }

TEST_F(JoinHashTableTest, ParallelBuildTest) {
  constexpr bool use_concise_ht = false;
  const uint32_t num_tuples = 10000;
  const uint32_t num_thread_local_tables = 4;

  MemoryPool memory(nullptr);
  ThreadStateContainer container(&memory);

  container.Reset(
      sizeof(JoinHashTable),
      [](auto *ctx, auto *s) {
        new (s) JoinHashTable(reinterpret_cast<MemoryPool *>(ctx), sizeof(Tuple), use_concise_ht);
      },
      [](auto *ctx, auto *s) { reinterpret_cast<JoinHashTable *>(s)->~JoinHashTable(); }, &memory);

  // Parallel populate each of the thread-local hash tables
  LaunchParallel(num_thread_local_tables, [&](auto tid) {
    auto *jht = container.AccessCurrentThreadStateAs<JoinHashTable>();
    PopulateJoinHashTable(jht, num_tuples, 1);
  });

  JoinHashTable main_jht(&memory, sizeof(Tuple), false);
  main_jht.MergeParallel(&container, 0);

  // Each of the thread-local tables inserted the same data, i.e., tuples whose
  // keys are in the range [0, num_tuples). Thus, in the final table there
  // should be num_thread_local_tables * num_tuples keys, where each of the
  // num_tuples tuples have num_thread_local_tables duplicates.
  //
  // Check now.

  EXPECT_EQ(num_tuples * num_thread_local_tables, main_jht.GetTupleCount());

  for (uint32_t i = 0; i < num_tuples; i++) {
    auto probe = Tuple{i, 1, 2, 3};
    uint32_t count = 0;
    for (auto iter = main_jht.Lookup<use_concise_ht>(probe.Hash()); iter.HasNext();) {
      auto *matched = reinterpret_cast<const Tuple *>(iter.GetMatchPayload());
      if (matched->a == probe.a) {
        count++;
      }
    }
    EXPECT_EQ(num_thread_local_tables, count);
  }
}

TEST_F(JoinHashTableTest, IterationTest) {
  const auto check_iteration_for_size = [](const std::size_t size) {
    // The join table.
    MemoryPool memory(nullptr);
    JoinHashTable join_hash_table(&memory, sizeof(Tuple), false);

    // Populate.
    for (uint32_t i = 0; i < size; i++) {
      auto tuple = Tuple{i, 1, 2, 44};
      auto space = join_hash_table.AllocInputTuple(tuple.Hash());
      *reinterpret_cast<Tuple *>(space) = tuple;
    }

    // Build.
    join_hash_table.Build();

    // Iterate.
    uint32_t count = 0;
    for (auto iter = JoinHashTableIterator(join_hash_table); iter.HasNext(); iter.Next()) {
      auto table_tuple = iter.GetCurrentRowAs<Tuple>();
      EXPECT_EQ(count++, table_tuple->a);
      EXPECT_EQ(1, table_tuple->b);
      EXPECT_EQ(2, table_tuple->c);
      EXPECT_EQ(44, table_tuple->d);
    }

    EXPECT_EQ(size, count) << "Expected " << size << " tuples in table. Counted: " << count;
  };

  // Empty table.
  check_iteration_for_size(0);

  // Some power-of-two sizes.
  for (uint32_t p = 3; p < 10; p++) {
    check_iteration_for_size(1u << p);
  }

  // Some random prime-sized tables.
  for (uint32_t size : {134639, 1071223}) {
    check_iteration_for_size(size);
  }
}

TEST_F(JoinHashTableTest, IterateParallelMergedTableTest) {
  constexpr uint32_t kNumThreadLocalTables = 4;

  // The state struct.
  class State {
   public:
    // Constructor.
    State(MemoryPool *memory) : jht_(memory, sizeof(Tuple), false) {}
    // JHT access.
    JoinHashTable *JHT() { return &jht_; }
    // Byte offset of table in state.
    static std::size_t JHTByteOffset() { return 0; }

   private:
    JoinHashTable jht_;
  };

  // The list of table sizes to construct.
  using TableSizes = std::array<uint32_t, kNumThreadLocalTables>;

  // Accepts a list of N integers where each integer value is the size of a hash
  // table to build. Each of the N hash tables are built in parallel. After
  // construction, the table is scanned and an expected count is confirmed.
  const auto build_parallel_and_check_iteration = [](TableSizes table_sizes) {
    // Thread-local container setup.
    MemoryPool memory(nullptr);
    ThreadStateContainer container(&memory);
    container.Reset(
        sizeof(State),
        [](auto ctx, auto space) { new (space) State(reinterpret_cast<MemoryPool *>(ctx)); },
        [](auto ctx, auto space) { std::destroy_at(reinterpret_cast<State *>(space)); }, &memory);

    // Make and populate thread-local tables.
    LaunchParallel(kNumThreadLocalTables, [&](auto tid) {
      const auto size = table_sizes[tid];
      auto join_hash_table = container.AccessCurrentThreadStateAs<State>()->JHT();
      for (uint32_t i = 0; i < size; i++) {
        auto tuple = Tuple{i, tid, 2, 44};
        auto space = join_hash_table->AllocInputTuple(tuple.Hash());
        *reinterpret_cast<Tuple *>(space) = tuple;
      }
    });

    // Merge.
    JoinHashTable main(&memory, sizeof(Tuple), false);
    main.MergeParallel(&container, State::JHTByteOffset());

    // Check.
    uint32_t count = 0;
    uint32_t expected_count = std::accumulate(table_sizes.begin(), table_sizes.end(), 0);
    for (auto iter = JoinHashTableIterator(main); iter.HasNext(); iter.Next()) {
      count++;
    }

    EXPECT_EQ(expected_count, count)
        << "Expected " << expected_count << " tuples in table. Counted: " << count;
  };

  // All empty tables.
  build_parallel_and_check_iteration({0, 0, 0, 0});
  // Some empty tables.
  build_parallel_and_check_iteration({0, 1, 0, 2});
  // Some random powers of two.
  build_parallel_and_check_iteration({1u << 13u, 1u << 3u, 0, 1u << 12u});
  // Some random primes and powers of two.
  build_parallel_and_check_iteration({479, 15013, 137, 5857});
}

#if 0
TEST_F(JoinHashTableTest, PerfTest) {
  const uint32_t num_tuples = 10000000;

  auto bench = [this](bool concise, uint32_t num_tuples) {
    JoinHashTable join_hash_table(memory(), sizeof(Tuple), concise);

    //
    // Build random input
    //

    std::random_device random;
    for (uint32_t i = 0; i < num_tuples; i++) {
      auto key = random();
      auto *tuple = reinterpret_cast<Tuple *>(
          join_hash_table.AllocInputTuple(util::Hasher::Hash(key)));

      tuple->a = key;
      tuple->b = i + 1;
      tuple->c = i + 2;
      tuple->d = i + 3;
    }

    util::Timer<std::milli> timer;
    timer.Start();

    join_hash_table.Build();

    timer.Stop();

    auto mtps = (num_tuples / timer.elapsed()) / 1000.0;
    auto size_in_kb =
        (concise ? ConciseTableFor(&join_hash_table)->GetTotalMemoryUsage()
                 : GenericTableFor(&join_hash_table)->GetTotalMemoryUsage()) /
        1024.0;
    LOG_INFO("========== {} ==========", concise ? "Concise" : "Generic");
    LOG_INFO("# Tuples    : {}", num_tuples)
    LOG_INFO("Table size  : {} KB", size_in_kb);
    LOG_INFO("Insert+Build: {} ms ({:.2f} Mtps)", timer.elapsed(), mtps);
  };

  bench(false, num_tuples);
  bench(true, num_tuples);
}
#endif

}  // namespace tpl::sql
