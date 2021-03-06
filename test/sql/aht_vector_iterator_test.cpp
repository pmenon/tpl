#include <memory>
#include <random>
#include <unordered_set>
#include <utility>
#include <vector>

#include "util/test_harness.h"

#include "sql/aggregation_hash_table.h"
#include "sql/constant_vector.h"
#include "sql/execution_context.h"
#include "sql/vector_operations/vector_operations.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/hash_util.h"

namespace tpl::sql {

/**
 * An input tuple, this is what we use to probe and update aggregates
 */
struct InputTuple {
  int64_t key, col_a;

  InputTuple(uint64_t key, uint64_t col_a) : key(key), col_a(col_a) {}

  hash_t Hash() const noexcept { return util::HashUtil::HashMurmur(key); }
};

/**
 * This is the tuple tracking aggregate values. It simulates:
 *
 * SELECT key, SUM(col_a), SUM(col_a*2), SUM(col_a*10) ...
 */
struct AggTuple {
  int64_t key, count1, count2, count3;

  explicit AggTuple(const InputTuple &input) : key(input.key), count1(0), count2(0), count3(0) {
    Advance(input);
  }

  void Advance(const InputTuple &input) {
    count1++;
    count2 += input.col_a * 2;
    count3 += input.col_a * 10;
  }
};

static void Transpose(const HashTableEntry *agg_entries[], const uint64_t size,
                      VectorProjectionIterator *const vpi) {
  for (uint32_t i = 0; i < size; i++, vpi->Advance()) {
    const auto *agg = agg_entries[i]->PayloadAs<AggTuple>();
    vpi->SetValue<int64_t, false>(0, agg->key, false);
    vpi->SetValue<int64_t, false>(1, agg->count1, false);
    vpi->SetValue<int64_t, false>(2, agg->count2, false);
    vpi->SetValue<int64_t, false>(3, agg->count3, false);
  }
}

class AggregationHashTableVectorIteratorTest : public TplTest {
 public:
  AggregationHashTableVectorIteratorTest() {
    memory_ = std::make_unique<MemoryPool>(nullptr);
    std::vector<Schema::ColumnInfo> cols = {
        {"key", Type::BigIntType(false)},
        {"count1", Type::BigIntType(false)},
        {"count2", Type::BigIntType(false)},
        {"count3", Type::BigIntType(false)},
    };
    schema_ = std::make_unique<Schema>(std::move(cols));
  }

  MemoryPool *Memory() { return memory_.get(); }

  std::vector<const Schema::ColumnInfo *> OutputSchema() {
    std::vector<const Schema::ColumnInfo *> ret;
    for (const auto &col : schema_->GetColumns()) {
      ret.push_back(&col);
    }
    return ret;
  }

  static void PopulateAggHT(AggregationHashTable *aht, const uint32_t num_aggs,
                            const uint32_t num_rows, uint32_t cola = 1) {
    for (uint32_t i = 0; i < num_rows; i++) {
      auto input = InputTuple(i % num_aggs, cola);
      AggTuple *existing = nullptr;
      for (auto *entry = aht->Lookup(input.Hash()); entry != nullptr; entry = entry->next) {
        auto tmp = entry->PayloadAs<AggTuple>();
        if (tmp->key == input.key) {
          existing = tmp;
          break;
        }
      }

      if (existing == nullptr) {
        new (aht->AllocInputTuple(input.Hash())) AggTuple(input);
      } else {
        existing->Advance(input);
      }
    }
  }

 private:
  std::unique_ptr<MemoryPool> memory_;
  std::unique_ptr<Schema> schema_;
};

TEST_F(AggregationHashTableVectorIteratorTest, IterateEmptyAggregation) {
  // Empty table
  AggregationHashTable agg_ht(Memory(), sizeof(AggTuple));

  // Iterate
  AHTVectorIterator iter(agg_ht, OutputSchema(), Transpose);
  for (; iter.HasNext(); iter.Next(Transpose)) {
    FAIL() << "Iteration should not occur on empty aggregation hash table";
  }
}

TEST_F(AggregationHashTableVectorIteratorTest, IterateSmallAggregation) {
  constexpr uint32_t num_aggs = 400;
  constexpr uint32_t group_size = 10;
  constexpr uint32_t num_tuples = num_aggs * group_size;

  // Insert 'num_tuples' tuples into an aggregation table to force the creation
  // of 'num_aggs' unique aggregates. Each aggregate should receive 'group_size'
  // tuples as input whose column value is 'cola'. The key range of aggregates
  // is [0, num_aggs).
  //
  // We need to ensure:
  // 1. After transposition, we receive exactly 'num_aggs' unique aggregates.
  // 2. For each aggregate, the associated count/sums is correct.

  AggregationHashTable agg_ht(Memory(), sizeof(AggTuple));

  // Populate
  PopulateAggHT(&agg_ht, num_aggs, num_tuples, 1 /* cola */);

  std::unordered_set<uint64_t> reference;

  // Iterate
  AHTVectorIterator iter(agg_ht, OutputSchema(), Transpose);
  for (; iter.HasNext(); iter.Next(Transpose)) {
    auto *vpi = iter.GetVectorProjectionIterator();
    EXPECT_FALSE(vpi->IsFiltered());
    EXPECT_EQ(4u, vpi->GetVectorProjection()->GetColumnCount());
    EXPECT_GT(vpi->GetVectorProjection()->GetTotalTupleCount(), 0);

    for (; vpi->HasNext(); vpi->Advance()) {
      auto agg_key = *vpi->GetValue<int64_t, false>(0, nullptr);
      auto agg_count_1 = *vpi->GetValue<int64_t, false>(1, nullptr);
      auto agg_count_2 = *vpi->GetValue<int64_t, false>(2, nullptr);
      auto agg_count_3 = *vpi->GetValue<int64_t, false>(3, nullptr);
      EXPECT_LT(agg_key, num_aggs);
      EXPECT_EQ(group_size, agg_count_1);
      EXPECT_EQ(agg_count_1 * 2u, agg_count_2);
      EXPECT_EQ(agg_count_1 * 10u, agg_count_3);
      EXPECT_EQ(0u, reference.count(agg_key));
      reference.insert(agg_key);
    }
  }

  EXPECT_EQ(num_aggs, reference.size());
}

TEST_F(AggregationHashTableVectorIteratorTest, FilterPostAggregation) {
  constexpr uint32_t num_aggs = 4000;
  constexpr uint32_t group_size = 10;
  constexpr uint32_t num_tuples = num_aggs * group_size;

  constexpr int32_t agg_needle_key = 686;

  // Insert 'num_tuples' tuples into an aggregation table to force the creation
  // of 'num_aggs' unique aggregates. Each aggregate should receive 'group_size'
  // tuples as input whose column value is 'cola'. The key range of aggregates
  // is [0, num_aggs).
  //
  // Apply a filter to the output of the iterator looking for a unique key.
  // There should only be one match since keys are unique.

  AggregationHashTable agg_ht(Memory(), sizeof(AggTuple));

  PopulateAggHT(&agg_ht, num_aggs, num_tuples, 1 /* cola */);

  // Iterate
  uint32_t num_needle_matches = 0;
  AHTVectorIterator iter(agg_ht, OutputSchema(), Transpose);
  for (; iter.HasNext(); iter.Next(Transpose)) {
    auto *vpi = iter.GetVectorProjectionIterator();
    vpi->RunFilter([&]() {
      auto agg_key = *vpi->GetValue<int64_t, false>(0, nullptr);
      return agg_key == agg_needle_key;
    });
    num_needle_matches += vpi->GetSelectedTupleCount();
  }

  EXPECT_EQ(1u, num_needle_matches);
}

TEST_F(AggregationHashTableVectorIteratorTest, DISABLED_Perf) {
  uint64_t taat_ret = 0, vaat_ret = 0;

  for (uint32_t size : {10, 100, 1000, 10000, 100000, 1000000, 10000000}) {
    // The table
    AggregationHashTable agg_ht(Memory(), sizeof(AggTuple));

    // Populate
    PopulateAggHT(&agg_ht, size, size * 10, 1 /* cola */);

    constexpr int32_t filter_val = 1000;

    auto vaat_ms = Bench(4, [&]() {
      vaat_ret = 0;
      TupleIdList tids(kDefaultVectorSize);
      AHTVectorIterator iter(agg_ht, OutputSchema(), Transpose);
      for (; iter.HasNext(); iter.Next(Transpose)) {
        auto vec_proj = iter.GetVectorProjectionIterator()->GetVectorProjection();
        tids.Resize(vec_proj->GetTotalTupleCount());
        tids.AddAll();
        VectorOps::SelectLessThan(*vec_proj->GetColumn(0),
                                  ConstantVector(GenericValue::CreateBigInt(filter_val)), &tids);
        vec_proj->SetFilteredSelections(tids);
      }
    });

    auto taat_ms = Bench(4, [&]() {
      taat_ret = 0;
      AHTIterator iter(agg_ht);
      for (; iter.HasNext(); iter.Next()) {
        auto *agg_row = reinterpret_cast<const AggTuple *>(iter.GetCurrentAggregateRow());
        if (agg_row->key < filter_val) {
          taat_ret++;
        }
      }
    });

    LOG_INFO("===== Size {} =====", size);
    LOG_INFO("Taat: {:.2f} ms ({}), Vaat: {:.2f} ({})", taat_ms, taat_ret, vaat_ms, vaat_ret);
  }
}

}  // namespace tpl::sql
