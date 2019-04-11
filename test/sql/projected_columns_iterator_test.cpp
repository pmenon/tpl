#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "catalog/catalog.h"
#include "sql/execution_structures.h"
#include "sql/projected_columns_iterator.h"

namespace tpl::sql::test {

///
/// This test uses a ProjectedColumns with four columns. The first column,
/// named "col_a" is a non-nullable small integer column whose values are
/// monotonically increasing. The second column, "col_b", is a nullable integer
/// column whose values are random. The third column, "col_c", is a non-nullable
/// integer column whose values are in the range [0, 1000). The last column,
/// "col_d", is a nullable big-integer column whose values are random.
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
  u32 num_nulls = 0;

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

class ProjectedColumnsIteratorTest : public TplTest {
 protected:
  enum ColId : u8 { col_a = 0, col_b = 1, col_c = 2, col_d = 3 };

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
  ProjectedColumnsIteratorTest() : num_tuples_(kDefaultVectorSize) {
    // NOTE: the storage layer reoder's these by size. So let's filter them now.
    auto cola_data = CreateMonotonicallyIncreasing<i16>(num_tuples());
    auto colb_data = CreateRandom<i32>(num_tuples());
    auto colb_null = CreateRandomNullBitmap(num_tuples());
    auto colc_data = CreateRandom<i32>(num_tuples(), 0, 1000);
    auto cold_data = CreateRandom<i64>(num_tuples());
    auto cold_null = CreateRandomNullBitmap(num_tuples());

    data_.emplace_back(std::move(cola_data), nullptr, 0, num_tuples());
    data_.emplace_back(std::move(colb_data), std::move(colb_null.first),
                       colb_null.second, num_tuples());
    data_.emplace_back(std::move(colc_data), nullptr, 0, num_tuples());
    data_.emplace_back(std::move(cold_data), std::move(cold_null.first),
                       cold_null.second, num_tuples());

    InitializeColumns();
    //  Fill up data
    for (u16 col_idx = 0; col_idx < data_.size(); col_idx++) {
      auto &[data, nulls, num_nulls, num_tuples] = data_[col_idx];
      (void)num_nulls;
      // Size of an element (e.g. 4 for u32).
      u32 elem_size =
          info_->GetStorageSchema()->GetColumns()[col_idx].GetAttrSize();
      u16 col_offset = GetColOffset(static_cast<ColId>(col_idx));
      std::cout << "Col Offset = " << (col_offset) << std::endl;
      projected_columns_->SetNumTuples(num_tuples);
      if (nulls != nullptr) {
        // Fill up the null bitmap.
        std::memcpy(projected_columns_->ColumnNullBitmap(col_offset),
                    nulls.get(), num_tuples);
      } else {
        // Set all rows to non-null.
        std::memset(projected_columns_->ColumnNullBitmap(col_offset), 0,
                    num_tuples);
      }
      // Fill up the values.
      std::memcpy(projected_columns_->ColumnStart(col_offset), data.get(),
                  elem_size * num_tuples);
    }
  }

  void InitializeColumns() {
    auto *exec = sql::ExecutionStructures::Instance();
    auto *ctl = exec->GetCatalog();
    // TODO(Amadou): Come up with an easier way to create ProjectedColumns.
    // This should be done after the catalog PR is merged in.
    // Create column metadata for every column.
    catalog::Schema::Column storage_col_a =
        ctl->MakeStorageColumn("col_a", sql::SmallIntType::Instance(false));
    catalog::Schema::Column storage_col_b =
        ctl->MakeStorageColumn("col_b", sql::IntegerType::Instance(true));
    catalog::Schema::Column storage_col_c =
        ctl->MakeStorageColumn("col_c", sql::IntegerType::Instance(false));
    catalog::Schema::Column storage_col_d =
        ctl->MakeStorageColumn("col_d", sql::BigIntType::Instance(true));
    sql::Schema::ColumnInfo sql_col_a("col_a",
                                      sql::SmallIntType::Instance(false));
    sql::Schema::ColumnInfo sql_col_b("col_b",
                                      sql::IntegerType::Instance(true));
    sql::Schema::ColumnInfo sql_col_c("col_c",
                                      sql::IntegerType::Instance(false));
    sql::Schema::ColumnInfo sql_col_d("col_d", sql::BigIntType::Instance(true));

    // Create the table in the catalog.
    catalog::Schema storage_schema(std::vector<catalog::Schema::Column>{
        storage_col_a, storage_col_b, storage_col_c, storage_col_d});
    sql::Schema sql_schema(std::vector<sql::Schema::ColumnInfo>{
        sql_col_a, sql_col_b, sql_col_c, sql_col_d});
    ctl->CreateTable("hash_test_table", std::move(storage_schema),
                     std::move(sql_schema));

    // Get the table's information.
    info_ = ctl->LookupTableByName("hash_test_table");
    std::vector<catalog::col_oid_t> col_oids;
    for (const auto col : info_->GetStorageSchema()->GetColumns())
      col_oids.emplace_back(col.GetOid());

    // Create a ProjectedColumns
    auto initializer_map = info_->GetTable()->InitializerForProjectedColumns(
        col_oids, kDefaultVectorSize);
    projection_map_ = initializer_map.second;
    buffer_ = common::AllocationUtil::AllocateAligned(
        initializer_map.first.ProjectedColumnsSize());
    projected_columns_ = initializer_map.first.Initialize(buffer_);
    projected_columns_->SetNumTuples(kDefaultVectorSize);
  }

  void SetSize(u32 size) { projected_columns_->SetNumTuples(size); }

 protected:
  u32 num_tuples() const { return num_tuples_; }

  storage::ProjectedColumns *GetProjectedColumn() { return projected_columns_; }

  catalog::Catalog::TableInfo *GetTableInfo() { return info_; }

  u16 GetColOffset(ColId col) {
    return projection_map_
        [info_->GetStorageSchema()->GetColumns()[col].GetOid()];
  }

  const ColData &column_data(u32 col_idx) const { return data_[col_idx]; }

 private:
  u32 num_tuples_;
  std::vector<ColData> data_;
  catalog::Catalog::TableInfo *info_ = nullptr;
  byte *buffer_ = nullptr;
  storage::ProjectedColumns *projected_columns_ = nullptr;
  storage::ProjectionMap projection_map_;
};

TEST_F(ProjectedColumnsIteratorTest, EmptyIteratorTest) {
  //
  // Check to see that iteration doesn't begin without an input block
  //

  ProjectedColumnsIterator iter(GetProjectedColumn(),
                                GetTableInfo()->GetSqlSchema());
  SetSize(0);

  for (; iter.HasNext(); iter.Advance()) {
    FAIL() << "Should not iterate with empty ProjectedColumns!";
  }
}

TEST_F(ProjectedColumnsIteratorTest, SimpleIteratorTest) {
  //
  // Check to see that iteration iterates over all tuples in the projection
  //

  {
    u32 tuple_count = 0;
    ProjectedColumnsIterator iter(GetProjectedColumn(),
                                  GetTableInfo()->GetSqlSchema());
    SetSize(kDefaultVectorSize);

    for (; iter.HasNext(); iter.Advance()) {
      tuple_count++;
    }

    EXPECT_EQ(kDefaultVectorSize, tuple_count);
    EXPECT_FALSE(iter.IsFiltered());
  }

  //
  // Check to see that column A is monotonically increasing
  //

  {
    ProjectedColumnsIterator iter(GetProjectedColumn(),
                                  GetTableInfo()->GetSqlSchema());
    SetSize(kDefaultVectorSize);

    bool entered = false;
    for (i16 last = -1; iter.HasNext(); iter.Advance()) {
      entered = true;
      auto *ptr = iter.Get<i16, false>(GetColOffset(ColId::col_a), nullptr);
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

TEST_F(ProjectedColumnsIteratorTest, ReadNullableColumnsTest) {
  //
  // Check to see that we can correctly count all NULL values in NULLable cols
  //

  ProjectedColumnsIterator iter(GetProjectedColumn(),
                                GetTableInfo()->GetSqlSchema());
  SetSize(kDefaultVectorSize);

  u32 num_nulls = 0;
  for (; iter.HasNext(); iter.Advance()) {
    bool null = false;
    auto *ptr = iter.Get<i32, true>(GetColOffset(ColId::col_b), &null);
    EXPECT_NE(nullptr, ptr);
    num_nulls += static_cast<u32>(null);
  }

  EXPECT_EQ(column_data(ColId::col_b).num_nulls, num_nulls);
}

TEST_F(ProjectedColumnsIteratorTest, ManualFilterTest) {
  //
  // Check to see that we can correctly manually apply a single filter on a
  // single column. We apply the filter IS_NOT_NULL(col_b)
  //

  {
    ProjectedColumnsIterator iter(GetProjectedColumn(),
                                  GetTableInfo()->GetSqlSchema());
    SetSize(kDefaultVectorSize);

    for (; iter.HasNext(); iter.Advance()) {
      bool null = false;
      iter.Get<i32, true>(GetColOffset(ColId::col_b), &null);
      iter.Match(!null);
    }

    iter.ResetFiltered();

    // Now all selected/active elements must not be null
    u32 num_non_null = 0;
    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.Get<i32, true>(GetColOffset(ColId::col_b), &null);
      EXPECT_FALSE(null);
      num_non_null++;
    }

    const auto &col_data = column_data(ColId::col_b);
    u32 actual_non_null = col_data.num_tuples - col_data.num_nulls;
    EXPECT_EQ(actual_non_null, num_non_null);
    EXPECT_EQ(actual_non_null, iter.num_selected());
  }

  //
  // Now try to apply filters individually on separate columns. Let's try:
  //
  // WHERE col_a < 100 and IS_NULL(col_b)
  //
  // The first filter (col_a < 100) should return 100 rows since it's a
  // monotonically increasing column. The second filter returns a
  // non-deterministic number of tuples, but it should be less than or equal to
  // 100. We'll do a manual check to be sure.
  //

  {
    ProjectedColumnsIterator iter(GetProjectedColumn(),
                                  GetTableInfo()->GetSqlSchema());
    SetSize(kDefaultVectorSize);

    for (; iter.HasNext(); iter.Advance()) {
      auto *val = iter.Get<i16, false>(GetColOffset(ColId::col_a), nullptr);
      iter.Match(*val < 100);
    }

    iter.ResetFiltered();

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.Get<i32, true>(GetColOffset(ColId::col_b), &null);
      iter.Match(null);
    }

    iter.ResetFiltered();

    EXPECT_LE(iter.num_selected(), 100u);

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      // col_a must be less than 100
      {
        auto *val = iter.Get<i16, false>(GetColOffset(ColId::col_a), nullptr);
        EXPECT_LT(*val, 100);
      }

      // col_b must be NULL
      {
        bool null = false;
        iter.Get<i32, true>(GetColOffset(ColId::col_b), &null);
        EXPECT_TRUE(null);
      }
    }
  }
}

TEST_F(ProjectedColumnsIteratorTest, ManagedFilterTest) {
  //
  // Check to see that we can correctly apply a single filter on a single
  // column using PCI's managed filter. We apply the filter IS_NOT_NULL(col_b)
  //

  ProjectedColumnsIterator iter(GetProjectedColumn(),
                                GetTableInfo()->GetSqlSchema());
  SetSize(kDefaultVectorSize);

  iter.RunFilter([&iter, this]() {
    bool null = false;
    iter.Get<i32, true>(this->GetColOffset(ColId::col_b), &null);
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
    iter.ForEach([&iter, &c, this]() {
      c++;
      bool null = false;
      iter.Get<i32, true>(this->GetColOffset(ColId::col_b), &null);
      EXPECT_FALSE(null);
    });

    EXPECT_EQ(actual_non_null, iter.num_selected());
    EXPECT_EQ(iter.num_selected(), c);
  }
}

TEST_F(ProjectedColumnsIteratorTest, SimpleVectorizedFilterTest) {
  //
  // Check to see that we can correctly apply a single vectorized filter. Here
  // we just check col_c < 100
  //

  ProjectedColumnsIterator iter(GetProjectedColumn(),
                                GetTableInfo()->GetSqlSchema());
  SetSize(kDefaultVectorSize);

  // Compute expected result
  u32 expected = 0;
  for (; iter.HasNext(); iter.Advance()) {
    auto val = *iter.Get<i32, false>(GetColOffset(ColId::col_c), nullptr);
    if (val < 100) {
      expected++;
    }
  }

  // Filter
  iter.FilterColByVal<std::less>(GetColOffset(ColId::col_c),
                                 sql::IntegerType::Instance(false),
                                 ProjectedColumnsIterator::FilterVal{.i = 100});

  // Check
  u32 count = 0;
  for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
    auto val = *iter.Get<i32, false>(GetColOffset(ColId::col_c), nullptr);
    EXPECT_LT(val, 100);
    count++;
  }

  EXPECT_EQ(expected, count);
}

TEST_F(ProjectedColumnsIteratorTest, MultipleVectorizedFilterTest) {
  //
  // Apply two filters in order:
  //  - col_c < 750
  //  - col_a < 10
  //
  // The first filter will return a non-deterministic result because it is a
  // randomly generated column. But, the second filter will return at most 10
  // results because it is a monotonically increasing column beginning at 0.
  //

  ProjectedColumnsIterator iter(GetProjectedColumn(),
                                GetTableInfo()->GetSqlSchema());
  SetSize(kDefaultVectorSize);

  iter.FilterColByVal<std::less>(GetColOffset(ColId::col_c),
                                 sql::IntegerType::Instance(false),
                                 ProjectedColumnsIterator::FilterVal{.i = 750});
  iter.FilterColByVal<std::less>(GetColOffset(ColId::col_a),
                                 sql::SmallIntType::Instance(false),
                                 ProjectedColumnsIterator::FilterVal{.si = 10});

  // Check
  u32 count = 0;
  for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
    auto col_a_val = *iter.Get<i16, false>(GetColOffset(ColId::col_a), nullptr);
    auto col_c_val = *iter.Get<i32, false>(GetColOffset(ColId::col_c), nullptr);
    std::cout << col_a_val << std::endl;
    ASSERT_LT(col_a_val, 10);
    EXPECT_LT(col_c_val, 750);
    count++;
  }

  LOG_INFO("Selected: {}", count);
  EXPECT_LE(count, 10u);
}

}  // namespace tpl::sql::test
