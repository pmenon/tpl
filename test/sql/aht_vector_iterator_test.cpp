#include <memory>
#include <random>
#include <unordered_set>
#include <utility>
#include <vector>

#include "tpl_test.h"  // NOLINT

// From test
#include "vm/module_compiler.h"

#include "sql/aggregation_hash_table.h"
#include "sql/execution_context.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/hash.h"
#include "vm/module.h"

namespace tpl::sql::test {

/**
 * An input tuple, this is what we use to probe and update aggregates
 */
struct InputTuple {
  u64 key, col_a;

  InputTuple(u64 key, u64 col_a) : key(key), col_a(col_a) {}

  hash_t Hash() const noexcept { return util::Hasher::Hash(key); }
};

/**
 * This is the tuple tracking aggregate values. It simulates:
 *
 * SELECT key, SUM(col_a), SUM(col_a*2), SUM(col_a*10) ...
 */
struct AggTuple {
  u64 key, count1, count2, count3;

  explicit AggTuple(const InputTuple &input)
      : key(input.key), count1(0), count2(0), count3(0) {
    Advance(input);
  }

  void Advance(const InputTuple &input) {
    count1++;
    count2 += input.col_a * 2;
    count3 += input.col_a * 10;
  }
};

// The function to determine whether an aggregate stored in the hash table and
// an input have equivalent keys.
static bool AggTupleKeyEq(const void *table_tuple, const void *probe_tuple) {
  auto *lhs = reinterpret_cast<const AggTuple *>(table_tuple);
  auto *rhs = reinterpret_cast<const InputTuple *>(probe_tuple);
  return lhs->key == rhs->key;
}

static void Transpose(const byte **raw_aggregates, const u64 size,
                      VectorProjectionIterator *const vpi) {
  auto **aggs = reinterpret_cast<const AggTuple **>(raw_aggregates);
  for (u32 i = 0; i < size; i++, vpi->Advance()) {
    const auto *agg = aggs[i];
    vpi->SetValue<u64, false>(0, agg->key, false);
    vpi->SetValue<u64, false>(1, agg->count1, false);
    vpi->SetValue<u64, false>(2, agg->count2, false);
    vpi->SetValue<u64, false>(3, agg->count3, false);
  }
}

class AggregationHashTableVectorIteratorTest : public TplTest {
 public:
  AggregationHashTableVectorIteratorTest() {
    memory_ = std::make_unique<MemoryPool>(nullptr);
    std::vector<Schema::ColumnInfo> cols = {
        {"key", BigIntType::InstanceNonNullable()},
        {"count1", BigIntType::InstanceNonNullable()},
        {"count2", BigIntType::InstanceNonNullable()},
        {"count3", BigIntType::InstanceNonNullable()},
    };
    schema_ = std::make_unique<Schema>(std::move(cols));
  }

  MemoryPool *memory() { return memory_.get(); }

  std::vector<const Schema::ColumnInfo *> output_schema() {
    std::vector<const Schema::ColumnInfo *> ret;
    for (const auto &col : schema_->columns()) {
      ret.push_back(&col);
    }
    return ret;
  }

  static void PopulateAggHT(AggregationHashTable *aht, const u32 num_aggs,
                            const u32 num_rows, u32 cola = 1) {
    for (u32 i = 0; i < num_rows; i++) {
      auto input = InputTuple(i % num_aggs, cola);
      auto existing = reinterpret_cast<AggTuple *>(
          aht->Lookup(input.Hash(), AggTupleKeyEq, &input));
      if (existing == nullptr) {
        new (aht->Insert(input.Hash())) AggTuple(input);
      } else {
        existing->Advance(input);
      }
    }
  }

 private:
  std::unique_ptr<MemoryPool> memory_;
  std::unique_ptr<Schema> schema_;
  std::vector<std::unique_ptr<vm::Module>> modules_;
};

TEST_F(AggregationHashTableVectorIteratorTest, IterateSmallAggregation) {
  constexpr u32 num_aggs = 4000;
  constexpr u32 group_size = 10;
  constexpr u32 num_tuples = num_aggs * group_size;

  //
  // Insert 'num_tuples' into an aggregation table to force the creation of
  // 'num_aggs' unique aggregates. Each aggregate should receive 'group_size'
  // tuples as input whose column value is 'cola'. The key range of aggregates
  // is [0, num_aggs).
  //
  // We need to ensure:
  // 1. After transposition, we receive exactly 'num_aggs' unique aggregates.
  // 2. For each aggregate, the associated count/sums is correct.
  //

  AggregationHashTable agg_ht(memory(), sizeof(AggTuple));

  // Populate
  PopulateAggHT(&agg_ht, num_aggs, num_tuples, 1 /* cola */);

  std::unordered_set<u64> reference;

  // Iterate
  AHTVectorIterator iter(agg_ht, output_schema(), Transpose);
  for (; iter.HasNext(); iter.Next(Transpose)) {
    auto *vpi = iter.GetVectorProjectionIterator();
    EXPECT_FALSE(vpi->IsFiltered());

    for (; vpi->HasNext(); vpi->Advance()) {
      auto agg_key = *vpi->GetValue<u64, false>(0, nullptr);
      auto agg_count_1 = *vpi->GetValue<u64, false>(1, nullptr);
      auto agg_count_2 = *vpi->GetValue<u64, false>(2, nullptr);
      auto agg_count_3 = *vpi->GetValue<u64, false>(3, nullptr);
      EXPECT_TRUE(agg_key < num_aggs);
      EXPECT_EQ(group_size, agg_count_1);
      EXPECT_EQ(agg_count_1 * 2u, agg_count_2);
      EXPECT_EQ(agg_count_1 * 10u, agg_count_3);
      // The key should be unique, i.e., one we haven't seen so far.
      EXPECT_EQ(0u, reference.count(agg_key));
      reference.insert(agg_key);
    }
  }

  EXPECT_EQ(num_aggs, reference.size());
}

TEST_F(AggregationHashTableVectorIteratorTest, FilterPostAggregation) {
  constexpr u32 num_aggs = 4000;
  constexpr u32 group_size = 10;
  constexpr u32 num_tuples = num_aggs * group_size;

  AggregationHashTable agg_ht(memory(), sizeof(AggTuple));

  PopulateAggHT(&agg_ht, num_aggs, num_tuples, 1 /* cola */);

  constexpr u32 agg_needle_key = 686;
  constexpr u32 agg_max_key = 2600;

  // Iterate
  u32 num_needle_keys = 0, num_keys_lt_max = 0;
  AHTVectorIterator iter(agg_ht, output_schema(), Transpose);
  for (; iter.HasNext(); iter.Next(Transpose)) {
    auto *vpi = iter.GetVectorProjectionIterator();
    vpi->ForEach([&]() {
      auto agg_key = *vpi->GetValue<u64, false>(0, nullptr);
      num_needle_keys += static_cast<u32>(agg_key == agg_needle_key);
      num_keys_lt_max += static_cast<u32>(agg_key < agg_max_key);
    });
  }

  // After filter, there should be exactly one key equal to the needle. Since we
  // use a dense aggregate key range
  EXPECT_EQ(1u, num_needle_keys);
  EXPECT_EQ(agg_max_key, num_keys_lt_max);
}

TEST_F(AggregationHashTableVectorIteratorTest, DISABLED_Perf) {
  u64 taat_ret = 0, vaat_ret = 0;

  for (u32 size : {10, 100, 1000, 10000, 100000, 1000000, 10000000}) {
    // The table
    AggregationHashTable agg_ht(memory(), sizeof(AggTuple));

    // Populate
    PopulateAggHT(&agg_ht, size, size * 10, 1 /* cola */);

    constexpr u32 filter = 1000;

    auto vaat_ms = Bench(4, [&]() {
      vaat_ret = 0;
      AHTVectorIterator iter(agg_ht, output_schema(), Transpose);
      for (; iter.HasNext(); iter.Next(Transpose)) {
        auto *vpi = iter.GetVectorProjectionIterator();
        auto val = VectorProjectionIterator::FilterVal{.bi = filter};
        vaat_ret += vpi->FilterColByVal<std::less>(0, val);
      }
    });

    auto taat_ms = Bench(4, [&]() {
      taat_ret = 0;
      AHTIterator iter(agg_ht);
      for (; iter.HasNext(); iter.Next()) {
        auto *agg_row =
            reinterpret_cast<const AggTuple *>(iter.GetCurrentAggregateRow());
        if (agg_row->key < static_cast<u64>(filter)) {
          taat_ret++;
        }
      }
    });

    LOG_INFO("===== Size {} =====", size);
    LOG_INFO("Taat: {:.2f} ms ({}), Vaat: {:.2f} ({})", taat_ms, taat_ret,
             vaat_ms, vaat_ret);
  }
}

}  // namespace tpl::sql::test
