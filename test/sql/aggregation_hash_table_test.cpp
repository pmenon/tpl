#include <memory>
#include <random>
#include <unordered_map>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "sql/aggregation_hash_table.h"
#include "sql/schema.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/hash.h"

namespace tpl::sql::test {

/**
 * An input tuple, this is what we use to probe and update aggregates
 */
struct InputTuple {
  u64 key, col_a;

  explicit InputTuple(u64 key, u64 col_a) : key(key), col_a(col_a) {}

  hash_t Hash() const noexcept {
    return util::Hasher::Hash(reinterpret_cast<const u8 *>(&key), sizeof(key));
  }
};

/**
 * This is the tuple tracking aggregate values
 */
struct AggTuple {
  u64 key, count1, count2, count3;

  explicit AggTuple(const InputTuple &input)
      : key(input.key), count1(0), count2(0), count3(0) {
    Advance(input);
  }

  void Advance(const InputTuple &input) {
    count1 += input.col_a;
    count2 += input.col_a * 2;
    count3 += input.col_a * 10;
  }

  bool operator==(const AggTuple &other) {
    return key == other.key && count1 == other.count1 &&
           count2 == other.count2 && count3 == other.count3;
  }
};

/// The function to determine whether two tuples have equivalent keys
static inline bool TupleKeyEq(const void *table_tuple,
                              const void *probe_tuple) {
  auto *lhs = reinterpret_cast<const AggTuple *>(table_tuple);
  auto *rhs = reinterpret_cast<const InputTuple *>(probe_tuple);
  return lhs->key == rhs->key;
}

class AggregationHashTableTest : public TplTest {
 public:
  AggregationHashTableTest()
      : memory_(nullptr), agg_table_(&memory_, sizeof(AggTuple)) {}

  MemoryPool *memory() { return &memory_; }

  AggregationHashTable *agg_table() { return &agg_table_; }

 private:
  MemoryPool memory_;
  AggregationHashTable agg_table_;
};

TEST_F(AggregationHashTableTest, SimpleRandomInsertionTest) {
  const u32 num_tuples = 10000;

  // The reference table
  std::unordered_map<u64, std::unique_ptr<AggTuple>> ref_agg_table;

  std::mt19937 generator;
  std::uniform_int_distribution<u64> distribution(0, 10);

  // Insert a few random tuples
  for (u32 idx = 0; idx < num_tuples; idx++) {
    auto input = InputTuple(distribution(generator), 1);
    auto hash_val = input.Hash();
    auto *existing = reinterpret_cast<AggTuple *>(agg_table()->Lookup(
        hash_val, TupleKeyEq, reinterpret_cast<const void *>(&input)));

    if (existing != nullptr) {
      // The reference table should have an equivalent aggregate tuple
      auto ref_iter = ref_agg_table.find(input.key);
      EXPECT_TRUE(ref_iter != ref_agg_table.end());
      EXPECT_TRUE(*ref_iter->second == *existing);
      existing->Advance(input);
      ref_iter->second->Advance(input);
    } else {
      // The reference table shouldn't have the aggregate
      auto ref_iter = ref_agg_table.find(input.key);
      if (ref_iter != ref_agg_table.end()) {
        FAIL();
      }
      EXPECT_TRUE(ref_iter == ref_agg_table.end());
      new (agg_table()->Insert(hash_val)) AggTuple(input);
      ref_agg_table.emplace(input.key, std::make_unique<AggTuple>(input));
    }
  }
}

TEST_F(AggregationHashTableTest, IterationTest) {
  //
  // SELECT key, SUM(cola), SUM(cola*2), SUM(cola*10) FROM table GROUP BY key
  //
  // Keys are selected continuously from the range [0, 10); all cola values are
  // equal to 1.
  //
  // Each group will receive G=num_inserts/10 tuples, count1 will be G,
  // count2 will be G*2, and count3 will be G*10.
  //

  const u32 num_inserts = 10000;
  const u32 num_groups = 10;
  const u32 tuples_per_group = num_inserts / num_groups;
  ASSERT_TRUE(num_inserts % num_groups == 0);

  {
    for (u32 idx = 0; idx < num_inserts; idx++) {
      InputTuple input(idx % num_groups, 1);
      auto *existing = reinterpret_cast<AggTuple *>(agg_table()->Lookup(
          input.Hash(), TupleKeyEq, reinterpret_cast<const void *>(&input)));

      if (existing != nullptr) {
        existing->Advance(input);
      } else {
        auto *new_agg = agg_table()->Insert(input.Hash());
        new (new_agg) AggTuple(input);
      }
    }
  }

  //
  // Iterate resulting aggregates. There should be exactly 10
  //

  {
    u32 group_count = 0;
    for (AggregationHashTableIterator iter(*agg_table()); iter.HasNext();
         iter.Next()) {
      auto *agg_tuple =
          reinterpret_cast<const AggTuple *>(iter.GetCurrentAggregateRow());
      EXPECT_EQ(tuples_per_group, agg_tuple->count1);
      EXPECT_EQ(tuples_per_group * 2, agg_tuple->count2);
      EXPECT_EQ(tuples_per_group * 10, agg_tuple->count3);
      group_count++;
    }

    EXPECT_EQ(num_groups, group_count);
  }
}

TEST_F(AggregationHashTableTest, SimplePartitionedInsertionTest) {
  const u32 num_tuples = 10000;

  std::mt19937 generator;
  std::uniform_int_distribution<u64> distribution(0, 10);

  // Insert a few random tuples
  for (u32 idx = 0; idx < num_tuples; idx++) {
    InputTuple input(distribution(generator), 1);
    auto *existing = reinterpret_cast<AggTuple *>(agg_table()->Lookup(
        input.Hash(), TupleKeyEq, reinterpret_cast<const void *>(&input)));

    if (existing != nullptr) {
      // The reference table should have an equivalent aggregate tuple
      existing->Advance(input);
    } else {
      // The reference table shouldn't have the aggregate
      auto *new_agg = agg_table()->InsertPartitioned(input.Hash());
      new (new_agg) AggTuple(input);
    }
  }
}

TEST_F(AggregationHashTableTest, BatchProcessTest) {
  const u32 num_groups = 16;

  const auto hash_fn = [](void *x) {
    auto iters = reinterpret_cast<VectorProjectionIterator **>(x);
    auto key = iters[0]->Get<u32, false>(0, nullptr);
    return util::Hasher::Hash(reinterpret_cast<const u8 *>(key), sizeof(u32));
  };

  const auto key_eq = [](const void *agg, const void *x) {
    auto agg_tuple = reinterpret_cast<const AggTuple *>(agg);
    auto iters = reinterpret_cast<const VectorProjectionIterator *const *>(x);
    auto vpi_key = iters[0]->Get<u32, false>(0, nullptr);
    return agg_tuple->key == *vpi_key;
  };

  const auto init_agg = [](void *agg, void *x) {
    auto iters = reinterpret_cast<VectorProjectionIterator **>(x);
    auto key = iters[0]->Get<u32, false>(0, nullptr);
    auto val = iters[0]->Get<u32, false>(1, nullptr);
    new (agg) AggTuple(InputTuple(*key, *val));
  };

  const auto advance_agg = [](void *agg, void *x) {
    auto agg_tuple = reinterpret_cast<AggTuple *>(agg);
    auto iters = reinterpret_cast<VectorProjectionIterator **>(x);
    auto key = iters[0]->Get<u32, false>(0, nullptr);
    auto val = iters[0]->Get<u32, false>(1, nullptr);
    agg_tuple->Advance(InputTuple(*key, *val));
  };

  Schema::ColumnInfo key_col("key", IntegerType::InstanceNonNullable());
  Schema::ColumnInfo val_col("val", IntegerType::InstanceNonNullable());
  std::vector<const Schema::ColumnInfo *> cols = {&key_col, &val_col};

  VectorProjection vp(cols, kDefaultVectorSize);

  alignas(CACHELINE_SIZE) u32 keys[kDefaultVectorSize];
  alignas(CACHELINE_SIZE) u32 vals[kDefaultVectorSize];

  for (u32 run = 0; run < 10; run++) {
    // Fill keys and value
    std::random_device random;
    for (u32 idx = 0; idx < kDefaultVectorSize; idx++) {
      keys[idx] = idx % num_groups;
      vals[idx] = 1;
    }

    // Setup projection
    vp.ResetFromRaw(reinterpret_cast<byte *>(keys), nullptr, 0,
                    kDefaultVectorSize);
    vp.ResetFromRaw(reinterpret_cast<byte *>(vals), nullptr, 1,
                    kDefaultVectorSize);

    // Process
    VectorProjectionIterator vpi(&vp);
    VectorProjectionIterator *iters[] = {&vpi};
    agg_table()->ProcessBatch(iters, hash_fn, key_eq, init_agg, advance_agg);
  }
}

}  // namespace tpl::sql::test
