#include "tpl_test.h"

#include <random>

#include "sql/join_hash_table.h"
#include "util/hash.h"

namespace tpl::sql::test {

/// This is the tuple we insert into the hash table
struct Tuple {
  u64 a, b, c, d;
};

/// The function to determine whether two tuples have equivalent keys
static inline bool TupleKeyEq(UNUSED void *_, void *probe_tuple,
                              void *table_tuple) {
  auto *lhs = reinterpret_cast<const Tuple *>(probe_tuple);
  auto *rhs = reinterpret_cast<const Tuple *>(table_tuple);
  return lhs->a == rhs->a;
}

class JoinHashTableTest : public TplTest {
 public:
  JoinHashTableTest() : region_(GetTestName()) {}

  util::Region *region() { return &region_; }

  GenericHashTable *GenericTableFor(JoinHashTable *join_hash_table) {
    return &join_hash_table->generic_hash_table_;
  }

  ConciseHashTable *ConciseTableFor(JoinHashTable *join_hash_table) {
    return &join_hash_table->concise_hash_table_;
  }

  BloomFilter *BloomFilterFor(JoinHashTable *join_hash_table) {
    return &join_hash_table->bloom_filter_;
  }

 private:
  util::Region region_;
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

  JoinHashTable join_hash_table(region(), sizeof(Tuple));

  // The table
  for (const auto &tuple : tuples) {
    auto hash_val = util::Hasher::Hash((const u8 *)&tuple.a, sizeof(tuple.a));
    auto *space = join_hash_table.AllocInputTuple(hash_val);
    *reinterpret_cast<Tuple *>(space) = tuple;
  }

  // Before build, the generic hash table shouldn't be populated, but the join
  // table's storage should have buffered all input tuples
  EXPECT_EQ(num_tuples, join_hash_table.num_elems());
  EXPECT_EQ(0u, GenericTableFor(&join_hash_table)->num_elements());

  // Try to build
  join_hash_table.Build();

  // Post-build, the sizes should be synced up since all tuples were inserted
  // into the GHT
  EXPECT_EQ(num_tuples, join_hash_table.num_elems());
  EXPECT_EQ(num_tuples, GenericTableFor(&join_hash_table)->num_elements());
}

TEST_F(JoinHashTableTest, UniqueKeyLookupTest) {
  // Test data
  const u32 num_tuples = 10;

  // The join table
  JoinHashTable join_hash_table(region(), sizeof(Tuple));

  // Some inserts of unique keys
  for (u32 i = 0; i < num_tuples; i++) {
    auto hash_val = util::Hasher::Hash((const u8 *)&i, sizeof(i));
    auto *space = join_hash_table.AllocInputTuple(hash_val);
    auto *tuple = reinterpret_cast<Tuple *>(space);

    tuple->a = i + 0;
    tuple->b = i + 1;
    tuple->c = i + 2;
    tuple->d = i + 3;
  }

  //
  // Build
  //

  join_hash_table.Build();

  //
  // Do some successful lookups
  //

  for (u32 i = 0; i < num_tuples; i++) {
    auto hash_val = util::Hasher::Hash((const u8 *)&i, sizeof(i));
    Tuple probe_tuple = {i, 0, 0, 0};
    u32 count = 0;
    HashTableEntry *entry = nullptr;
    for (auto iter = join_hash_table.Lookup(hash_val);
         (entry = iter.NextMatch(TupleKeyEq, nullptr, (void *)&probe_tuple));) {
      // Check contents
      auto *matched = reinterpret_cast<Tuple *>(entry->payload);
      EXPECT_EQ(i + 0, matched->a);
      EXPECT_EQ(i + 1, matched->b);
      EXPECT_EQ(i + 2, matched->c);
      EXPECT_EQ(i + 3, matched->d);
      count++;
    }
    EXPECT_EQ(1u, count)
        << "Expected to find only a single match for unique keys, but key ["
        << i << "] found " << count << " matches";
  }

  //
  // Do some unsuccessful lookups.
  //

  for (u32 i = num_tuples; i < num_tuples + 1000; i++) {
    auto hash_val = util::Hasher::Hash((const u8 *)&i, sizeof(i));
    Tuple probe_tuple = {i, 0, 0, 0};
    for (auto iter = join_hash_table.Lookup(hash_val);
         iter.NextMatch(TupleKeyEq, nullptr, (void *)&probe_tuple);) {
      FAIL() << "Should not find any matches for key [" << i
             << "] that was not inserted into the join hash table";
    }
  }
}

TEST_F(JoinHashTableTest, DuplicateKeyLookupTest) {
  // Test data
  const u32 num_tuples = 10;
  const u32 num_dups = 5;

  // The join table
  JoinHashTable join_hash_table(region(), sizeof(Tuple));

  // Some inserts with repetitions
  for (u32 rep = 0; rep < num_dups; rep++) {
    for (u32 i = 0; i < num_tuples; i++) {
      auto hash_val = util::Hasher::Hash((const u8 *)&i, sizeof(i));
      auto *space = join_hash_table.AllocInputTuple(hash_val);
      auto *tuple = reinterpret_cast<Tuple *>(space);

      tuple->a = i + 0;
      tuple->b = i + 1;
      tuple->c = i + 2;
      tuple->d = i + 3;
    }
  }

  join_hash_table.Build();

  //
  // Do some successful lookups
  //

  for (u32 i = 0; i < num_tuples; i++) {
    auto hash_val = util::Hasher::Hash((const u8 *)&i, sizeof(i));
    Tuple probe_tuple = {i, 0, 0, 0};
    u32 count = 0;
    for (auto iter = join_hash_table.Lookup(hash_val);
         iter.NextMatch(TupleKeyEq, nullptr, (void *)&probe_tuple);) {
      count++;
    }
    EXPECT_EQ(num_dups, count) << "Find " << count << " matches for key [" << i
                               << "], expected " << num_dups;
  }
}

TEST_F(JoinHashTableTest, UniqueKeyConciseTableTest) {
  // Test data
  const u32 num_tuples = 400;

  // The join table
  JoinHashTable join_hash_table(region(), sizeof(Tuple), true);

  //
  // Some inserts
  //

  for (u32 i = 0; i < num_tuples; i++) {
    auto hash_val = util::Hasher::Hash((const u8 *)&i, sizeof(i));
    auto *space = join_hash_table.AllocInputTuple(hash_val);
    auto *tuple = reinterpret_cast<Tuple *>(space);

    tuple->a = i + 0;
    tuple->b = i + 1;
    tuple->c = i + 2;
    tuple->d = i + 3;
  }

  //
  // Build
  //

  join_hash_table.Build();
}

TEST_F(JoinHashTableTest, DISABLED_PerfTest) {
  const u32 num_tuples = 10000000;

  auto bench = [this](bool concise, u32 num_tuples) {
    JoinHashTable join_hash_table(region(), sizeof(Tuple), concise);

    //
    // Build random input
    //

    std::random_device random;
    for (u32 i = 0; i < num_tuples; i++) {
      auto key = random();
      auto hash_val = util::Hasher::Hash((const u8 *)&key, sizeof(key),
                                         util::HashMethod::Crc);
      auto *space = join_hash_table.AllocInputTuple(hash_val);
      auto *tuple = reinterpret_cast<Tuple *>(space);

      tuple->a = hash_val;
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

}  // namespace tpl::sql::test
