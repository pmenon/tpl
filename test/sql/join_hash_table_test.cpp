#include <random>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include <tbb/tbb.h>  // NOLINT

#include "sql/join_hash_table.h"
#include "sql/thread_state_container.h"
#include "util/hash.h"

namespace tpl::sql {

/// This is the tuple we insert into the hash table
struct Tuple {
  u64 a, b, c, d;

  hash_t Hash() const { return util::Hasher::Hash(a); }
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
  const u32 num_tuples = 10;
  std::vector<Tuple> tuples(num_tuples);

  // Populate test data
  {
    std::mt19937 generator;
    std::uniform_int_distribution<u64> distribution;

    for (u32 i = 0; i < num_tuples; i++) {
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
  EXPECT_EQ(num_tuples, join_hash_table.GetElementCount());
  EXPECT_EQ(0u, join_hash_table.generic_hash_table_.num_elements());

  // Try to build
  join_hash_table.Build();

  // Post-build, the sizes should be synced up since all tuples were inserted
  // into the GHT
  EXPECT_EQ(num_tuples, join_hash_table.GetElementCount());
  EXPECT_EQ(num_tuples, join_hash_table.generic_hash_table_.num_elements());
}

void PopulateJoinHashTable(JoinHashTable *jht, u32 num_tuples,
                           u32 dup_scale_factor) {
  for (u32 rep = 0; rep < dup_scale_factor; rep++) {
    for (u32 i = 0; i < num_tuples; i++) {
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
void BuildAndProbeTest(u32 num_tuples, u32 dup_scale_factor) {
  //
  // The join table
  //

  MemoryPool memory(nullptr);
  JoinHashTable join_hash_table(&memory, sizeof(Tuple), UseCHT);

  //
  // Populate
  //

  PopulateJoinHashTable(&join_hash_table, num_tuples, dup_scale_factor);

  //
  // Build
  //

  join_hash_table.Build();

  //
  // Do some successful lookups
  //

  for (u32 i = 0; i < num_tuples; i++) {
    // The probe tuple
    Tuple probe_tuple = {i, 0, 0, 0};
    auto key_eq = [&](const Tuple *t) { return t->a == probe_tuple.a; };
    // Perform probe
    u32 count = 0;
    for (auto iter = join_hash_table.Lookup<UseCHT>(probe_tuple.Hash());
         iter.template HasNext<Tuple>(key_eq);) {
      auto *entry = iter.NextMatch();
      auto *matched = reinterpret_cast<const Tuple *>(entry->payload);
      EXPECT_EQ(i, matched->a);
      count++;
    }
    EXPECT_EQ(dup_scale_factor, count)
        << "Expected to find " << dup_scale_factor << " matches, but key [" << i
        << "] found " << count << " matches";
  }

  //
  // Do some unsuccessful lookups.
  //

  for (u32 i = num_tuples; i < num_tuples + 1000; i++) {
    // A tuple that should NOT find any join partners
    Tuple probe_tuple = {i, 0, 0, 0};
    auto key_eq = [&](const Tuple *t) { return t->a == probe_tuple.a; };
    // Lookups should fail
    for (auto iter = join_hash_table.Lookup<UseCHT>(probe_tuple.Hash());
         iter.template HasNext<Tuple>(key_eq);) {
      FAIL() << "Should not find any matches for key [" << i
             << "] that was not inserted into the join hash table";
    }
  }
}

TEST_F(JoinHashTableTest, UniqueKeyLookupTest) {
  BuildAndProbeTest<false>(400, 1);
}

TEST_F(JoinHashTableTest, DuplicateKeyLookupTest) {
  BuildAndProbeTest<false>(400, 5);
}

TEST_F(JoinHashTableTest, UniqueKeyConciseTableTest) {
  BuildAndProbeTest<true>(400, 1);
}

TEST_F(JoinHashTableTest, DuplicateKeyLookupConciseTableTest) {
  BuildAndProbeTest<true>(400, 5);
}

TEST_F(JoinHashTableTest, ParallelBuildTest) {
  constexpr bool use_concise_ht = false;
  const u32 num_tuples = 10000;
  const u32 num_thread_local_tables = 4;

  MemoryPool memory(nullptr);
  ThreadStateContainer container(&memory);

  container.Reset(sizeof(JoinHashTable),
                  [](auto *ctx, auto *s) {
                    new (s) JoinHashTable(reinterpret_cast<MemoryPool *>(ctx),
                                          sizeof(Tuple), use_concise_ht);
                  },
                  [](auto *ctx, auto *s) {
                    reinterpret_cast<JoinHashTable *>(s)->~JoinHashTable();
                  },
                  &memory);

  // Parallel populate each of the thread-local hash tables
  tbb::task_scheduler_init sched;
  tbb::blocked_range<std::size_t> block_range(0, num_thread_local_tables, 1);
  tbb::parallel_for(block_range, [&](const auto &range) {
    auto *jht = container.AccessThreadStateOfCurrentThreadAs<JoinHashTable>();
    LOG_INFO("JHT @ {:p}", (void *)jht);
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

  EXPECT_EQ(num_tuples * num_thread_local_tables, main_jht.GetElementCount());

  for (u32 i = 0; i < num_tuples; i++) {
    auto probe = Tuple{i, 1, 2, 3};
    auto key_eq = [&](const Tuple *t) { return t->a == probe.a; };

    u32 count = 0;
    for (auto iter = main_jht.Lookup<use_concise_ht>(probe.Hash());
         iter.HasNext<Tuple>(key_eq); iter.NextMatch()) {
      count++;
    }
    EXPECT_EQ(num_thread_local_tables, count);
  }
}

#if 0
TEST_F(JoinHashTableTest, PerfTest) {
  const u32 num_tuples = 10000000;

  auto bench = [this](bool concise, u32 num_tuples) {
    JoinHashTable join_hash_table(memory(), sizeof(Tuple), concise);

    //
    // Build random input
    //

    std::random_device random;
    for (u32 i = 0; i < num_tuples; i++) {
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
