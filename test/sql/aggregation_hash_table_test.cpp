#include <atomic>
#include <memory>
#include <random>
#include <unordered_map>
#include <vector>

#include "tbb/tbb.h"

#include "sql/aggregation_hash_table.h"
#include "sql/execution_context.h"
#include "sql/schema.h"
#include "sql/thread_state_container.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/hash_util.h"
#include "util/test_harness.h"

namespace tpl::sql {

/**
 * An input tuple, this is what we use to probe and update aggregates
 */
struct InputTuple {
  int64_t key, col_a;

  explicit InputTuple(uint64_t key, uint64_t col_a) : key(key), col_a(col_a) {}

  hash_t Hash() const noexcept { return util::HashUtil::Hash(key); }
};

/**
 * This is the tuple tracking aggregate values
 */
struct AggTuple {
  int64_t key;
  uint64_t count1, count2, count3;

  explicit AggTuple(const InputTuple &input) : key(input.key), count1(0), count2(0), count3(0) {
    Advance(input);
  }

  void Advance(const InputTuple &input) {
    count1 += input.col_a;
    count2 += input.col_a * 2;
    count3 += input.col_a * 10;
  }

  void Merge(const AggTuple &input) {
    count1 += input.count1;
    count2 += input.count2;
    count3 += input.count3;
  }

  bool operator==(const AggTuple &other) {
    return key == other.key && count1 == other.count1 && count2 == other.count2 &&
           count3 == other.count3;
  }
};

// The function to determine whether an aggregate stored in the hash table and
// an input have equivalent keys.
static inline bool AggTupleKeyEq(const void *table_tuple, const void *probe_tuple) {
  auto *lhs = reinterpret_cast<const AggTuple *>(table_tuple);
  auto *rhs = reinterpret_cast<const InputTuple *>(probe_tuple);
  return lhs->key == rhs->key;
}

// The function to determine whether two aggregates stored in overflow
// partitions or hash tables have equivalent keys.
static inline bool AggAggKeyEq(const void *agg_tuple_1, const void *agg_tuple_2) {
  auto *lhs = reinterpret_cast<const AggTuple *>(agg_tuple_1);
  auto *rhs = reinterpret_cast<const AggTuple *>(agg_tuple_2);
  return lhs->key == rhs->key;
}

class AggregationHashTableTest : public TplTest {
 public:
  AggregationHashTableTest() : memory_(nullptr), agg_table_(&memory_, sizeof(AggTuple)) {}

  MemoryPool *memory() { return &memory_; }

  AggregationHashTable *agg_table() { return &agg_table_; }

 private:
  MemoryPool memory_;
  AggregationHashTable agg_table_;
};

TEST_F(AggregationHashTableTest, SimpleRandomInsertionTest) {
  const uint32_t num_tuples = 10000;

  // The reference table
  std::unordered_map<uint64_t, std::unique_ptr<AggTuple>> ref_agg_table;

  std::mt19937 generator;
  std::uniform_int_distribution<uint64_t> distribution(0, 9);

  // Insert a few random tuples
  for (uint32_t idx = 0; idx < num_tuples; idx++) {
    auto input = InputTuple(distribution(generator), 1);
    auto hash_val = input.Hash();
    auto *existing = reinterpret_cast<AggTuple *>(
        agg_table()->Lookup(hash_val, AggTupleKeyEq, reinterpret_cast<const void *>(&input)));

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
      new (agg_table()->AllocInputTuple(hash_val)) AggTuple(input);
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

  const uint32_t num_inserts = 10000;
  const uint32_t num_groups = 10;
  const uint32_t tuples_per_group = num_inserts / num_groups;
  ASSERT_EQ(0u, num_inserts % num_groups);

  {
    for (uint32_t idx = 0; idx < num_inserts; idx++) {
      InputTuple input(idx % num_groups, 1);
      auto *existing = reinterpret_cast<AggTuple *>(
          agg_table()->Lookup(input.Hash(), AggTupleKeyEq, reinterpret_cast<const void *>(&input)));

      if (existing != nullptr) {
        existing->Advance(input);
      } else {
        auto *new_agg = agg_table()->AllocInputTuple(input.Hash());
        new (new_agg) AggTuple(input);
      }
    }
  }

  //
  // Iterate resulting aggregates. There should be exactly 10
  //

  {
    uint32_t group_count = 0;
    for (AHTIterator iter(*agg_table()); iter.HasNext(); iter.Next()) {
      auto *agg_tuple = reinterpret_cast<const AggTuple *>(iter.GetCurrentAggregateRow());
      EXPECT_EQ(tuples_per_group, agg_tuple->count1);
      EXPECT_EQ(tuples_per_group * 2, agg_tuple->count2);
      EXPECT_EQ(tuples_per_group * 10, agg_tuple->count3);
      group_count++;
    }

    EXPECT_EQ(num_groups, group_count);
  }
}

TEST_F(AggregationHashTableTest, SimplePartitionedInsertionTest) {
  const uint32_t num_tuples = 10000;

  std::mt19937 generator;
  std::uniform_int_distribution<uint64_t> distribution(0, 9);

  for (uint32_t idx = 0; idx < num_tuples; idx++) {
    InputTuple input(distribution(generator), 1);
    auto *existing = reinterpret_cast<AggTuple *>(
        agg_table()->Lookup(input.Hash(), AggTupleKeyEq, reinterpret_cast<const void *>(&input)));

    if (existing != nullptr) {
      existing->Advance(input);
    } else {
      auto *new_agg = agg_table()->AllocInputTuplePartitioned(input.Hash());
      new (new_agg) AggTuple(input);
    }
  }
}

TEST_F(AggregationHashTableTest, BatchProcessTest) {
  const uint32_t num_groups = 16;

  const auto hash_fn = [](void *x) {
    auto vpi = reinterpret_cast<const VectorProjectionIterator *>(x);
    auto key = vpi->GetValue<int32_t, false>(0, nullptr);
    return util::HashUtil::Hash(*key);
  };

  const auto key_eq = [](const void *agg, const void *x) {
    auto agg_tuple = reinterpret_cast<const AggTuple *>(agg);
    auto vpi = reinterpret_cast<const VectorProjectionIterator *>(x);
    auto vpi_key = vpi->GetValue<int32_t, false>(0, nullptr);
    return agg_tuple->key == *vpi_key;
  };

  const auto init_agg = [](void *agg, void *x) {
    auto vpi = reinterpret_cast<const VectorProjectionIterator *>(x);
    auto key = vpi->GetValue<int32_t, false>(0, nullptr);
    auto val = vpi->GetValue<int32_t, false>(1, nullptr);
    new (agg) AggTuple(InputTuple(*key, *val));
  };

  const auto advance_agg = [](void *agg, void *x) {
    auto vpi = reinterpret_cast<const VectorProjectionIterator *>(x);
    auto agg_tuple = reinterpret_cast<AggTuple *>(agg);
    auto key = vpi->GetValue<int32_t, false>(0, nullptr);
    auto val = vpi->GetValue<int32_t, false>(1, nullptr);
    agg_tuple->Advance(InputTuple(*key, *val));
  };

  Schema::ColumnInfo key_col("key", IntegerType::InstanceNonNullable());
  Schema::ColumnInfo val_col("val", IntegerType::InstanceNonNullable());
  std::vector<const Schema::ColumnInfo *> cols = {&key_col, &val_col};

  VectorProjection vp;
  vp.InitializeEmpty(cols);

  alignas(CACHELINE_SIZE) uint32_t keys[kDefaultVectorSize];
  alignas(CACHELINE_SIZE) uint32_t vals[kDefaultVectorSize];

  for (uint32_t run = 0; run < 10; run++) {
    // Fill keys and value
    std::random_device random;
    for (uint32_t idx = 0; idx < kDefaultVectorSize; idx++) {
      keys[idx] = idx % num_groups;
      vals[idx] = 1;
    }

    // Setup projection
    vp.ResetColumn(reinterpret_cast<byte *>(keys), nullptr, 0, kDefaultVectorSize);
    vp.ResetColumn(reinterpret_cast<byte *>(vals), nullptr, 1, kDefaultVectorSize);

    // Process
    VectorProjectionIterator vpi(&vp);
    agg_table()->ProcessBatch(&vpi, hash_fn, key_eq, init_agg, advance_agg, false);
  }
}

TEST_F(AggregationHashTableTest, OverflowPartitonIteratorTest) {
  struct Data {
    uint32_t key{5};
    uint32_t val{10};
  };

  struct TestEntry : public HashTableEntry {
    Data data;
    TestEntry() : HashTableEntry(), data{} {}
    TestEntry(uint32_t key, uint32_t val) : HashTableEntry(), data{key, val} {}
  };

  constexpr uint32_t nparts = 50;
  constexpr uint32_t nentries_per_part = 10;

  // Allocate partitions
  std::array<HashTableEntry *, nparts> partitions{};
  partitions.fill(nullptr);

  //
  // Test: check iteration over an empty partitions array
  //

  {
    uint32_t count = 0;
    AHTOverflowPartitionIterator iter(partitions.begin(), partitions.end());
    for (; iter.HasNext(); iter.Next()) {
      count++;
    }
    EXPECT_EQ(0u, count);
  }

  //
  // Test: insert one entry in the middle partition and ensure we find it
  //

  {
    std::vector<std::unique_ptr<TestEntry>> entries;
    entries.emplace_back(std::make_unique<TestEntry>(100, 200));

    HashTableEntry *entry = entries[0].get();
    const uint32_t part_idx = nparts / 2;
    entry->next = partitions[part_idx];
    partitions[part_idx] = entry;

    // Check
    uint32_t count = 0;
    AHTOverflowPartitionIterator iter(partitions.begin(), partitions.end());
    for (; iter.HasNext(); iter.Next()) {
      EXPECT_EQ(100u, iter.GetRowAs<Data>()->key);
      EXPECT_EQ(200u, iter.GetRowAs<Data>()->val);
      count++;
    }
    EXPECT_EQ(1u, count);
  }

  partitions.fill(nullptr);

  //
  // Test: create a list of nparts partitions and vary the number of entries in
  //       each partition from [0, nentries_per_part). Ensure the counts match.
  //

  {
    std::vector<std::unique_ptr<TestEntry>> entries;

    // Populate each partition
    std::random_device random;
    uint32_t num_entries = 0;
    for (uint32_t part_idx = 0; part_idx < nparts; part_idx++) {
      const uint32_t nentries = (random() % nentries_per_part);
      for (uint32_t i = 0; i < nentries; i++) {
        // Create entry
        entries.emplace_back(std::make_unique<TestEntry>());
        HashTableEntry *entry = entries[entries.size() - 1].get();

        // Link it into partition
        entry->next = partitions[part_idx];
        partitions[part_idx] = entry;
        num_entries++;
      }
    }

    // Check
    uint32_t count = 0;
    AHTOverflowPartitionIterator iter(partitions.begin(), partitions.end());
    for (; iter.HasNext(); iter.Next()) {
      count++;
    }
    EXPECT_EQ(num_entries, count);
  }
}

TEST_F(AggregationHashTableTest, ParallelAggregationTest) {
  tbb::task_scheduler_init sched;

  // Thread-local state initialization function:
  // Creates the aggregation table
  const auto init_ht = [](void *ctx, void *aht) {
    auto exec_ctx = reinterpret_cast<ExecutionContext *>(ctx);
    new (aht) AggregationHashTable(exec_ctx->GetMemoryPool(), sizeof(AggTuple));
  };

  // Thread-local state destruction function:
  // Destroys the aggregation hash table
  const auto destroy_ht = [](void *ctx, void *aht) {
    std::destroy_at(reinterpret_cast<AggregationHashTable *>(aht));
  };

  // Function to build one aggregation hash table. Inserts random data to ensure 'num_aggs'
  // unique aggregates in final table.
  constexpr uint32_t num_aggs = 100;
  const auto build_agg_table = [&](AggregationHashTable *agg_table) {
    std::mt19937 generator;
    std::uniform_int_distribution<uint64_t> distribution(0, num_aggs - 1);

    for (uint32_t idx = 0; idx < 10000; idx++) {
      InputTuple input(distribution(generator), 1);
      auto *existing = reinterpret_cast<AggTuple *>(
          agg_table->Lookup(input.Hash(), AggTupleKeyEq, reinterpret_cast<const void *>(&input)));
      if (existing != nullptr) {
        existing->Advance(input);
      } else {
        auto *new_agg = agg_table->AllocInputTuplePartitioned(input.Hash());
        new (new_agg) AggTuple(input);
      }
    }
  };

  // Merging function to merge overflow partition data
  auto merge = [](void *ctx, AggregationHashTable *table, AHTOverflowPartitionIterator *iter) {
    for (; iter->HasNext(); iter->Next()) {
      auto *partial_agg = iter->GetRowAs<AggTuple>();
      auto *existing =
          reinterpret_cast<AggTuple *>(table->Lookup(iter->GetRowHash(), AggAggKeyEq, partial_agg));
      if (existing != nullptr) {
        existing->Merge(*partial_agg);
      } else {
        table->Insert(iter->GetEntryForRow());
      }
    }
  };

  struct QueryState {
    std::atomic<uint32_t> row_count;
  };

  auto scan = [](void *query_state, void *thread_state, const AggregationHashTable *agg_table) {
    auto *qs = reinterpret_cast<QueryState *>(query_state);
    qs->row_count += agg_table->GetTupleCount();
  };

  QueryState query_state{0};
  MemoryPool memory(nullptr);
  ExecutionContext ctx(&memory);
  ThreadStateContainer container(&memory);

  // Build 4 thread-local aggregation hash tables
  container.Reset(sizeof(AggregationHashTable), init_ht, destroy_ht, &ctx);
  LaunchParallel(4, [&](auto tid) {
    build_agg_table(container.AccessThreadStateOfCurrentThreadAs<AggregationHashTable>());
  });

  // The main table that merges all thread-local tables
  AggregationHashTable main_table(&memory, sizeof(AggTuple));
  main_table.TransferMemoryAndPartitions(&container, 0, merge);

  // Clear thread-local container to ensure all memory has been moved
  container.Clear();

  // Scan main table and ensure all data exists.
  main_table.ExecuteParallelPartitionedScan(&query_state, &container, scan);

  // Check
  EXPECT_EQ(num_aggs, query_state.row_count.load(std::memory_order_seq_cst));
}

}  // namespace tpl::sql
