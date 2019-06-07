#include <memory>
#include <random>
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
  for (u32 i = 0; i < size; i++) {
    const auto *agg = aggs[i];
    vpi->SetValue<u64, false>(0, &agg->key, false);
    vpi->SetValue<u64, false>(1, &agg->count1, false);
    vpi->SetValue<u64, false>(2, &agg->count2, false);
    vpi->SetValue<u64, false>(3, &agg->count3, false);
  }
}

class AggregationHashTableVectorIteratorTest : public TplTest {
 public:
  AggregationHashTableVectorIteratorTest() {
    memory_ = std::make_unique<MemoryPool>(nullptr);
    aht_ =
        std::make_unique<AggregationHashTable>(memory_.get(), sizeof(AggTuple));
    std::vector<Schema::ColumnInfo> cols = {
        {"key", BigIntType::InstanceNonNullable()},
        {"count1", BigIntType::InstanceNonNullable()},
        {"count2", BigIntType::InstanceNonNullable()},
        {"count3", BigIntType::InstanceNonNullable()},
    };
    schema_ = std::make_unique<Schema>(std::move(cols));
  }

  AggregationHashTable *agg_table() { return aht_.get(); }

  std::vector<const Schema::ColumnInfo *> output_schema() {
    std::vector<const Schema::ColumnInfo *> ret;
    for (const auto &col : schema_->columns()) {
      ret.push_back(&col);
    }
    return ret;
  }

  void PopulateTestAggHT(const u32 num_aggs, const u32 num_rows) {
    std::random_device random;
    for (u32 i = 0; i < num_rows; i++) {
      auto input = InputTuple(random() % num_aggs, random() % 10);
      auto existing = reinterpret_cast<AggTuple *>(
          aht_->Lookup(input.Hash(), AggTupleKeyEq, &input));
      if (existing == nullptr) {
        new (aht_->Insert(input.Hash())) AggTuple(input);
      } else {
        existing->Advance(input);
      }
    }
  }

 private:
  std::unique_ptr<MemoryPool> memory_;
  std::unique_ptr<AggregationHashTable> aht_;
  std::unique_ptr<Schema> schema_;
  std::vector<std::unique_ptr<vm::Module>> modules_;
};

TEST_F(AggregationHashTableVectorIteratorTest, Transpose) {
  const u32 num_aggs = 100;

  // Populate
  PopulateTestAggHT(num_aggs, 1000);

  // Iterate
  AHTVectorIterator iter(*agg_table(), output_schema(), Transpose);
  for (; iter.HasNext(); iter.Next(Transpose)) {
    auto *vpi = iter.GetVectorProjectionIterator();
    EXPECT_EQ(num_aggs, vpi->num_selected());
  }
}

}  // namespace tpl::sql::test
