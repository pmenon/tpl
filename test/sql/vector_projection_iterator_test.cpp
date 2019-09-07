#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "sql/catalog.h"
#include "sql/vector_projection_iterator.h"
#include "util/test_harness.h"

namespace tpl::sql {

///
/// The tests in this file work from one VectorProjection with five columns:
///   col_a SMALLINT NOT NULL (Sequential)
///   col_b INTEGER (Random)
///   col_c INTEGER NOT NULL (Range [0,1000])
///   col_d BIGINT NOT NULL (Random)
///   col_e BIGINT NOT NULL (Range[0,100])
///   col_f BIGINT NOT NULL (Range [50,100])
///

namespace {

template <typename T>
std::unique_ptr<byte[]> CreateMonotonicallyIncreasing(uint32_t num_elems) {
  auto input = std::make_unique<byte[]>(num_elems * sizeof(T));
  auto *typed_input = reinterpret_cast<T *>(input.get());
  std::iota(typed_input, &typed_input[num_elems], static_cast<T>(0));
  return input;
}

template <typename T>
std::unique_ptr<byte[]> CreateRandom(uint32_t num_elems, T min = 0,
                                     T max = std::numeric_limits<T>::max()) {
  auto input = std::make_unique<byte[]>(num_elems * sizeof(T));

  std::mt19937 generator;
  std::uniform_int_distribution<T> distribution(min, max);

  auto *typed_input = reinterpret_cast<T *>(input.get());
  for (uint32_t i = 0; i < num_elems; i++) {
    typed_input[i] = distribution(generator);
  }

  return input;
}

std::pair<std::unique_ptr<uint32_t[]>, uint32_t> CreateRandomNullBitmap(uint32_t num_elems) {
  auto input = std::make_unique<uint32_t[]>(util::BitUtil::Num32BitWordsFor(num_elems));
  auto num_nulls = 0;

  std::mt19937 generator;
  std::uniform_int_distribution<uint32_t> distribution(0, 10);

  for (uint32_t i = 0; i < num_elems; i++) {
    if (distribution(generator) < 5) {
      util::BitUtil::Flip(input.get(), i);
      num_nulls++;
    }
  }

  return {std::move(input), num_nulls};
}

}  // namespace

class VectorProjectionIteratorTest : public TplTest {
 protected:
  // The columns
  enum ColId : uint8_t { col_a = 0, col_b = 1, col_c = 2, col_d = 3, col_e = 4, col_f = 5 };

  struct ColData {
    std::unique_ptr<byte[]> data;
    std::unique_ptr<uint32_t[]> nulls;
    uint32_t num_nulls;
    uint32_t num_tuples;

    ColData(std::unique_ptr<byte[]> data, std::unique_ptr<uint32_t[]> nulls, uint32_t num_nulls,
            uint32_t num_tuples)
        : data(std::move(data)),
          nulls(std::move(nulls)),
          num_nulls(num_nulls),
          num_tuples(num_tuples) {}
  };

 public:
  VectorProjectionIteratorTest() : num_tuples_(10) {
    // Create the schema
    std::vector<Schema::ColumnInfo> cols = {
        Schema::ColumnInfo("col_a", SmallIntType::InstanceNonNullable()),
        Schema::ColumnInfo("col_b", IntegerType::InstanceNullable()),
        Schema::ColumnInfo("col_c", IntegerType::InstanceNonNullable()),
        Schema::ColumnInfo("col_d", BigIntType::InstanceNonNullable()),
        Schema::ColumnInfo("col_e", BigIntType::InstanceNonNullable()),
        Schema::ColumnInfo("col_f", BigIntType::InstanceNonNullable()),
    };
    schema_ = std::make_unique<Schema>(std::move(cols));

    std::vector<const Schema::ColumnInfo *> column_info = {
        schema_->GetColumnInfo(0), schema_->GetColumnInfo(1), schema_->GetColumnInfo(2),
        schema_->GetColumnInfo(3), schema_->GetColumnInfo(4), schema_->GetColumnInfo(5),
    };
    vp_ = std::make_unique<VectorProjection>();
    vp_->InitializeEmpty(column_info);

    // Load the data
    LoadData();
  }

  void LoadData() {
    auto cola_data = CreateMonotonicallyIncreasing<int16_t>(num_tuples());
    auto colb_data = CreateRandom<int32_t>(num_tuples());
    auto [colb_null, colb_num_nulls] = CreateRandomNullBitmap(num_tuples());
    auto colc_data = CreateRandom<int32_t>(num_tuples(), 0, 1000);
    auto cold_data = CreateRandom<int64_t>(num_tuples());
    auto cole_data = CreateRandom<int64_t>(num_tuples(), 0, 100);
    auto colf_data = CreateRandom<int64_t>(num_tuples(), 50, 100);

    data_.emplace_back(std::move(cola_data), nullptr, 0, num_tuples());
    data_.emplace_back(std::move(colb_data), std::move(colb_null), colb_num_nulls, num_tuples());
    data_.emplace_back(std::move(colc_data), nullptr, 0, num_tuples());
    data_.emplace_back(std::move(cold_data), nullptr, 0, num_tuples());
    data_.emplace_back(std::move(cole_data), nullptr, 0, num_tuples());
    data_.emplace_back(std::move(colf_data), nullptr, 0, num_tuples());

    for (uint32_t col_idx = 0; col_idx < data_.size(); col_idx++) {
      auto &[data, nulls, num_nulls, num_tuples] = data_[col_idx];
      (void)num_nulls;
      vp_->ResetColumn(data.get(), nulls.get(), col_idx, num_tuples);
    }
  }

 protected:
  uint32_t num_tuples() const { return num_tuples_; }

  VectorProjection *vp() { return vp_.get(); }

  const ColData &column_data(uint32_t col_idx) const { return data_[col_idx]; }

 private:
  uint32_t num_tuples_;
  std::unique_ptr<Schema> schema_;
  std::unique_ptr<VectorProjection> vp_;
  std::vector<ColData> data_;
};

TEST_F(VectorProjectionIteratorTest, EmptyIteratorTest) {
  //
  // Test: check to see that iteration doesn't begin without an input block
  //

  VectorProjection empty_vector_proj;
  VectorProjectionIterator iter;
  iter.SetVectorProjection(&empty_vector_proj);

  for (; iter.HasNext(); iter.Advance()) {
    FAIL() << "Should not iterate with empty vector projection!";
  }
}

TEST_F(VectorProjectionIteratorTest, SimpleIteratorTest) {
  //
  // Test: check to see that iteration iterates over all tuples in the
  //       projection
  //

  {
    uint32_t tuple_count = 0;

    VectorProjectionIterator iter;
    iter.SetVectorProjection(vp());

    for (; iter.HasNext(); iter.Advance()) {
      tuple_count++;
    }

    EXPECT_EQ(vp()->GetTupleCount(), tuple_count);
    EXPECT_FALSE(iter.IsFiltered());
  }

  //
  // Check to see that column A is monotonically increasing
  //

  {
    VectorProjectionIterator iter;
    iter.SetVectorProjection(vp());

    bool entered = false;
    for (int16_t last = -1; iter.HasNext(); iter.Advance()) {
      entered = true;
      auto *ptr = iter.GetValue<int16_t, false>(ColId::col_a, nullptr);
      EXPECT_NE(nullptr, ptr);
      if (last != -1) {
        EXPECT_LE(last, *ptr);
      }
      last = *ptr;
    }

    EXPECT_TRUE(entered);
    EXPECT_FALSE(iter.IsFiltered());
  }
}

TEST_F(VectorProjectionIteratorTest, ReadNullableColumnsTest) {
  //
  // Test: check to see that we can correctly count all NULL values in NULLable
  //       cols
  //

  VectorProjectionIterator iter;
  iter.SetVectorProjection(vp());

  uint32_t num_nulls = 0;
  for (; iter.HasNext(); iter.Advance()) {
    bool null = false;
    iter.GetValue<int32_t, true>(ColId::col_b, &null);
    num_nulls += static_cast<uint32_t>(null);
  }

  EXPECT_EQ(column_data(ColId::col_b).num_nulls, num_nulls);
}

TEST_F(VectorProjectionIteratorTest, ManualFilterTest) {
  //
  // Test: check to see that we can correctly manually apply a single filter on
  //       a single column. We apply the filter IS_NOT_NULL(col_b)
  //

  {
    VectorProjectionIterator iter;
    iter.SetVectorProjection(vp());

    for (; iter.HasNext(); iter.Advance()) {
      bool null = false;
      iter.GetValue<int32_t, true>(ColId::col_b, &null);
      iter.Match(!null);
    }

    iter.ResetFiltered();

    // Now all selected/active elements must not be null
    uint32_t num_non_null = 0;
    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.GetValue<int32_t, true>(ColId::col_b, &null);
      EXPECT_FALSE(null);
      num_non_null++;
    }

    const auto &col_data = column_data(ColId::col_b);
    uint32_t actual_non_null = col_data.num_tuples - col_data.num_nulls;
    EXPECT_EQ(actual_non_null, num_non_null);
    EXPECT_EQ(actual_non_null, iter.GetTupleCount());
  }

  //
  // Test: try to apply filters individually on separate columns.
  //
  // Apply: WHERE col_a < 100 and IS_NULL(col_b)
  //
  // Expectation: The first filter (col_a < 100) should return 100 rows since
  //              it's a monotonically increasing column. The second filter
  //              returns a non-deterministic number of tuples, but it should be
  //              less than or equal to 100. We'll do a manual check to be sure.
  //

  {
    VectorProjectionIterator iter;
    iter.SetVectorProjection(vp());

    for (; iter.HasNext(); iter.Advance()) {
      auto *val = iter.GetValue<int16_t, false>(ColId::col_a, nullptr);
      iter.Match(*val < 100);
    }

    iter.ResetFiltered();

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.GetValue<int32_t, true>(ColId::col_b, &null);
      iter.Match(null);
    }

    iter.ResetFiltered();

    EXPECT_LE(iter.GetTupleCount(), 100u);

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      // col_a must be less than 100
      {
        auto *val = iter.GetValue<int16_t, false>(ColId::col_a, nullptr);
        EXPECT_LT(*val, 100);
      }

      // col_b must be NULL
      {
        bool null = false;
        iter.GetValue<int32_t, true>(ColId::col_b, &null);
        EXPECT_TRUE(null);
      }
    }
  }
}

TEST_F(VectorProjectionIteratorTest, ManagedFilterTest) {
  //
  // Test: check to see that we can correctly apply a single filter on a single
  // column using VPI's managed filter. We apply the filter IS_NOT_NULL(col_b)
  //

  VectorProjectionIterator iter;
  iter.SetVectorProjection(vp());

  iter.RunFilter([&iter]() {
    bool null = false;
    iter.GetValue<int32_t, true>(ColId::col_b, &null);
    return !null;
  });

  const auto &col_data = column_data(ColId::col_b);
  uint32_t actual_non_null = col_data.num_tuples - col_data.num_nulls;
  EXPECT_EQ(actual_non_null, iter.GetTupleCount());

  //
  // Ensure subsequent iterations only work on selected items
  //

  {
    uint32_t c = 0;
    iter.ForEach([&iter, &c]() {
      c++;
      bool null = false;
      iter.GetValue<int32_t, true>(ColId::col_b, &null);
      EXPECT_FALSE(null);
    });

    EXPECT_EQ(actual_non_null, iter.GetTupleCount());
    EXPECT_EQ(iter.GetTupleCount(), c);
  }
}

}  // namespace tpl::sql
