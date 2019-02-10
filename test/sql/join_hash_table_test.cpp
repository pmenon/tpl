#include "gtest/gtest.h"

#include <random>

#include "tpl_test.h"

#include "sql/runtime/join_hash_table.h"
#include "util/hash.h"

namespace tpl::sql::test {

class JoinHashTableTest : public TplTest {
 public:
  JoinHashTableTest() : region_(GetTestName()), join_hash_table_(region()) {}

  util::Region *region() { return &region_; }

  runtime::JoinHashTable *join_hash_table() { return &join_hash_table_; }

  runtime::GenericHashTable *inner_generic_hash_table() {
    return join_hash_table()->generic_hash_table();
  }

 private:
  util::Region region_;

  runtime::JoinHashTable join_hash_table_;
};

struct Tuple {
  u64 a, b, c, d;
};

static inline bool TupleKeyEq(UNUSED void *_, void *probe_tuple,
                              void *table_tuple) {
  auto *lhs = reinterpret_cast<const Tuple *>(probe_tuple);
  auto *rhs = reinterpret_cast<const Tuple *>(table_tuple);
  return lhs->a == rhs->a;
}

TEST_F(JoinHashTableTest, LazyInsertionTest) {
  // Test data
  u32 num_tuples = 10;
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

  // The table
  for (const auto &tuple : tuples) {
    auto hash_val = util::Hasher::Hash(reinterpret_cast<const u8 *>(&tuple.a),
                                       sizeof(tuple.a));
    auto *e = reinterpret_cast<Tuple *>(
        join_hash_table()->AllocInputTuple(hash_val, sizeof(Tuple)));
    *e = tuple;
  }

  EXPECT_EQ(num_tuples, join_hash_table()->num_elems());
  EXPECT_EQ(0u, inner_generic_hash_table()->num_elements());

  // Try to build

  join_hash_table()->Build();

  EXPECT_EQ(num_tuples, join_hash_table()->num_elems());
  EXPECT_EQ(num_tuples, inner_generic_hash_table()->num_elements());
}

TEST_F(JoinHashTableTest, UniqueKeyLookupTest) {
  // Test data
  u32 num_tuples = 10;

  // Some inserts
  for (u32 i = 0; i < num_tuples; i++) {
    auto hash_val = util::Hasher::Hash((const u8 *)&i, sizeof(i));
    auto *space = join_hash_table()->AllocInputTuple(hash_val, sizeof(Tuple));
    auto *tuple = reinterpret_cast<Tuple *>(space);

    tuple->a = i + 0;
    tuple->b = i + 1;
    tuple->c = i + 2;
    tuple->d = i + 3;
  }

  //
  // Build
  //

  join_hash_table()->Build();

  //
  // Do some successful lookups
  //

  for (u32 i = 0; i < num_tuples; i++) {
    auto hash_val = util::Hasher::Hash((const u8 *)&i, sizeof(i));
    Tuple probe_tuple = {i, 0, 0, 0};
    u32 count = 0;
    runtime::JoinHashTable::Entry *entry = nullptr;
    for (auto iter = join_hash_table()->Lookup(hash_val);
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
    for (auto iter = join_hash_table()->Lookup(hash_val);
         iter.NextMatch(TupleKeyEq, nullptr, (void *)&probe_tuple);) {
      FAIL() << "Should not find any matches for key [" << i
             << "] that was not inserted into the join hash table";
    }
  }
}

TEST_F(JoinHashTableTest, DuplicateKeyLookupTest) {
  // Test data
  u32 num_tuples = 10;
  u32 num_dups = 5;

  // Some inserts with repetitions
  for (u32 rep = 0; rep < num_dups; rep++) {
    for (u32 i = 0; i < num_tuples; i++) {
      auto hash_val = util::Hasher::Hash((const u8 *)&i, sizeof(i));
      auto *space = join_hash_table()->AllocInputTuple(hash_val, sizeof(Tuple));
      auto *tuple = reinterpret_cast<Tuple *>(space);

      tuple->a = i + 0;
      tuple->b = i + 1;
      tuple->c = i + 2;
      tuple->d = i + 3;
    }
  }

  join_hash_table()->Build();

  //
  // Do some successful lookups
  //

  for (u32 i = 0; i < num_tuples; i++) {
    auto hash_val = util::Hasher::Hash((const u8 *)&i, sizeof(i));
    Tuple probe_tuple = {i, 0, 0, 0};
    u32 count = 0;
    for (auto iter = join_hash_table()->Lookup(hash_val);
         iter.NextMatch(TupleKeyEq, nullptr, (void *)&probe_tuple);) {
      count++;
    }
    EXPECT_EQ(num_dups, count) << "Find " << count << " matches for key [" << i
                               << "], expected " << num_dups;
  }
}

}  // namespace tpl::sql::test