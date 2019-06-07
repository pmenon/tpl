#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "sql/catalog.h"
#include "sql/vector_projection_iterator.h"

namespace tpl::sql::test {

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
std::unique_ptr<byte[]> CreateMonotonicallyIncreasing(u32 num_elems) {
  auto input = std::make_unique<byte[]>(num_elems * sizeof(T));
  auto *typed_input = reinterpret_cast<T *>(input.get());
  std::iota(typed_input, &typed_input[num_elems], static_cast<T>(0));
  return input;
}

template <typename T>
std::unique_ptr<byte[]> CreateRandom(u32 num_elems, T min = 0,
                                     T max = std::numeric_limits<T>::max()) {
  auto input = std::make_unique<byte[]>(num_elems * sizeof(T));

  std::mt19937 generator;
  std::uniform_int_distribution<T> distribution(min, max);

  auto *typed_input = reinterpret_cast<T *>(input.get());
  for (u32 i = 0; i < num_elems; i++) {
    typed_input[i] = distribution(generator);
  }

  return input;
}

std::pair<std::unique_ptr<u32[]>, u32> CreateRandomNullBitmap(u32 num_elems) {
  auto input =
      std::make_unique<u32[]>(util::BitUtil::Num32BitWordsFor(num_elems));
  auto num_nulls = 0;

  std::mt19937 generator;
  std::uniform_int_distribution<u32> distribution(0, 10);

  for (u32 i = 0; i < num_elems; i++) {
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
  enum ColId : u8 {
    col_a = 0,
    col_b = 1,
    col_c = 2,
    col_d = 3,
    col_e = 4,
    col_f = 5
  };

  struct ColData {
    std::unique_ptr<byte[]> data;
    std::unique_ptr<u32[]> nulls;
    u32 num_nulls;
    u32 num_tuples;

    ColData(std::unique_ptr<byte[]> data, std::unique_ptr<u32[]> nulls,
            u32 num_nulls, u32 num_tuples)
        : data(std::move(data)),
          nulls(std::move(nulls)),
          num_nulls(num_nulls),
          num_tuples(num_tuples) {}
  };

 public:
  VectorProjectionIteratorTest() : num_tuples_(kDefaultVectorSize) {
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

    std::vector<const Schema::ColumnInfo *> vp_cols_info = {
        schema_->GetColumnInfo(0), schema_->GetColumnInfo(1),
        schema_->GetColumnInfo(2), schema_->GetColumnInfo(3),
        schema_->GetColumnInfo(4), schema_->GetColumnInfo(5),
    };
    vp_ = std::make_unique<VectorProjection>(vp_cols_info, num_tuples());

    // Load the data
    LoadData();
  }

  void LoadData() {
    auto cola_data = CreateMonotonicallyIncreasing<i16>(num_tuples());
    auto colb_data = CreateRandom<i32>(num_tuples());
    auto [colb_null, colb_num_nulls] = CreateRandomNullBitmap(num_tuples());
    auto colc_data = CreateRandom<i32>(num_tuples(), 0, 1000);
    auto cold_data = CreateRandom<i64>(num_tuples());
    auto cole_data = CreateRandom<i64>(num_tuples(), 0, 100);
    auto colf_data = CreateRandom<i64>(num_tuples(), 50, 100);

    data_.emplace_back(std::move(cola_data), nullptr, 0, num_tuples());
    data_.emplace_back(std::move(colb_data), std::move(colb_null),
                       colb_num_nulls, num_tuples());
    data_.emplace_back(std::move(colc_data), nullptr, 0, num_tuples());
    data_.emplace_back(std::move(cold_data), nullptr, 0, num_tuples());
    data_.emplace_back(std::move(cole_data), nullptr, 0, num_tuples());
    data_.emplace_back(std::move(colf_data), nullptr, 0, num_tuples());

    for (u32 col_idx = 0; col_idx < data_.size(); col_idx++) {
      auto &[data, nulls, num_nulls, num_tuples] = data_[col_idx];
      (void)num_nulls;
      vp_->ResetFromRaw(data.get(), nulls.get(), col_idx, num_tuples);
    }
  }

 protected:
  u32 num_tuples() const { return num_tuples_; }

  VectorProjection *vp() { return vp_.get(); }

  const ColData &column_data(u32 col_idx) const { return data_[col_idx]; }

 private:
  u32 num_tuples_;
  std::unique_ptr<Schema> schema_;
  std::unique_ptr<VectorProjection> vp_;
  std::vector<ColData> data_;
};

TEST_F(VectorProjectionIteratorTest, EmptyIteratorTest) {
  //
  // Test: check to see that iteration doesn't begin without an input block
  //

  std::vector<const Schema::ColumnInfo *> vp_col_info;
  VectorProjection empty_vecproj(vp_col_info, 0);
  VectorProjectionIterator iter;
  iter.SetVectorProjection(&empty_vecproj);

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
    u32 tuple_count = 0;

    VectorProjectionIterator iter;
    iter.SetVectorProjection(vp());

    for (; iter.HasNext(); iter.Advance()) {
      tuple_count++;
    }

    EXPECT_EQ(vp()->total_tuple_count(), tuple_count);
    EXPECT_FALSE(iter.IsFiltered());
  }

  //
  // Check to see that column A is monotonically increasing
  //

  {
    VectorProjectionIterator iter;
    iter.SetVectorProjection(vp());

    bool entered = false;
    for (i16 last = -1; iter.HasNext(); iter.Advance()) {
      entered = true;
      auto *ptr = iter.GetValue<i16, false>(ColId::col_a, nullptr);
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

  u32 num_nulls = 0;
  for (; iter.HasNext(); iter.Advance()) {
    bool null = false;
    auto *ptr = iter.GetValue<i32, true>(ColId::col_b, &null);
    EXPECT_NE(nullptr, ptr);
    num_nulls += static_cast<u32>(null);
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
      iter.GetValue<i32, true>(ColId::col_b, &null);
      iter.Match(!null);
    }

    iter.ResetFiltered();

    // Now all selected/active elements must not be null
    u32 num_non_null = 0;
    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.GetValue<i32, true>(ColId::col_b, &null);
      EXPECT_FALSE(null);
      num_non_null++;
    }

    const auto &col_data = column_data(ColId::col_b);
    u32 actual_non_null = col_data.num_tuples - col_data.num_nulls;
    EXPECT_EQ(actual_non_null, num_non_null);
    EXPECT_EQ(actual_non_null, iter.num_selected());
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
      auto *val = iter.GetValue<i16, false>(ColId::col_a, nullptr);
      iter.Match(*val < 100);
    }

    iter.ResetFiltered();

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.GetValue<i32, true>(ColId::col_b, &null);
      iter.Match(null);
    }

    iter.ResetFiltered();

    EXPECT_LE(iter.num_selected(), 100u);

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      // col_a must be less than 100
      {
        auto *val = iter.GetValue<i16, false>(ColId::col_a, nullptr);
        EXPECT_LT(*val, 100);
      }

      // col_b must be NULL
      {
        bool null = false;
        iter.GetValue<i32, true>(ColId::col_b, &null);
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
    iter.GetValue<i32, true>(ColId::col_b, &null);
    return !null;
  });

  const auto &col_data = column_data(ColId::col_b);
  u32 actual_non_null = col_data.num_tuples - col_data.num_nulls;
  EXPECT_EQ(actual_non_null, iter.num_selected());

  //
  // Ensure subsequent iterations only work on selected items
  //

  {
    u32 c = 0;
    iter.ForEach([&iter, &c]() {
      c++;
      bool null = false;
      iter.GetValue<i32, true>(ColId::col_b, &null);
      EXPECT_FALSE(null);
    });

    EXPECT_EQ(actual_non_null, iter.num_selected());
    EXPECT_EQ(iter.num_selected(), c);
  }
}

TEST_F(VectorProjectionIteratorTest, SimpleVectorizedFilterTest) {
  //
  // Test: check to see that we can correctly apply a single vectorized filter.
  //       Here we just check col_c < 100
  //

  VectorProjectionIterator iter;
  iter.SetVectorProjection(vp());

  // Compute expected result
  u32 expected = 0;
  for (; iter.HasNext(); iter.Advance()) {
    auto val = *iter.GetValue<i32, false>(ColId::col_c, nullptr);
    if (val < 100) {
      expected++;
    }
  }

  // Filter
  iter.FilterColByVal<std::less>(ColId::col_c,
                                 VectorProjectionIterator::FilterVal{.i = 100});

  // Check
  u32 count = 0;
  for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
    auto val = *iter.GetValue<i32, false>(ColId::col_c, nullptr);
    EXPECT_LT(val, 100);
    count++;
  }

  EXPECT_EQ(expected, count);
}

TEST_F(VectorProjectionIteratorTest, MultipleVectorizedFilterTest) {
  //
  // Apply two filters in order:
  //  - col_c < 750
  //  - col_a < 10
  //
  // The first filter will return a non-deterministic result because it is a
  // randomly generated column. But, the second filter will return at most 10
  // results because it is a monotonically increasing column beginning at 0.
  //

  VectorProjectionIterator iter;
  iter.SetVectorProjection(vp());

  iter.FilterColByVal<std::less>(ColId::col_c,
                                 VectorProjectionIterator::FilterVal{.i = 750});
  iter.FilterColByVal<std::less>(ColId::col_a,
                                 VectorProjectionIterator::FilterVal{.si = 10});

  // Check
  u32 count = 0;
  for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
    auto col_a_val = *iter.GetValue<i16, false>(ColId::col_a, nullptr);
    auto col_c_val = *iter.GetValue<i32, false>(ColId::col_c, nullptr);
    EXPECT_LT(col_a_val, 10);
    EXPECT_LT(col_c_val, 750);
    count++;
  }

  LOG_INFO("Selected: {}", count);
  EXPECT_LE(count, 10u);
}

TEST_F(VectorProjectionIteratorTest, FilterColByColTest) {
  //
  // Test: check we can apply a single filter on one column by the contents of
  //       another column. Both columns are the same type.
  //
  //       We first determine the correct count by iterating over the VP
  //       manually, then use the vectorized filter. The selected counts should
  //       match.
  //

#define CHECK(functor_op, op)                                                 \
  {                                                                           \
    VectorProjectionIterator iter;                                            \
    iter.SetVectorProjection(vp());                                           \
                                                                              \
    /* First check using scalar filter */                                     \
    u32 expected = 0;                                                         \
    for (; iter.HasNext(); iter.Advance()) {                                  \
      auto col_e_val = *iter.Get<i64, false>(ColId::col_e, nullptr);          \
      auto col_f_val = *iter.Get<i64, false>(ColId::col_f, nullptr);          \
      expected += static_cast<u32>(col_e_val op col_f_val);                   \
    }                                                                         \
                                                                              \
    /* Now apply vectorized filter */                                         \
    auto count = iter.FilterColByCol<functor_op>(ColId::col_e, ColId::col_f); \
    EXPECT_EQ(expected, count);                                               \
  }

  CHECK(std::equal_to, ==)
  CHECK(std::greater, >)
  CHECK(std::greater_equal, >=)
  CHECK(std::less, <)
  CHECK(std::less_equal, <=)
  CHECK(std::not_equal_to, !=)

#undef CHECK
}

}  // namespace tpl::sql::test
