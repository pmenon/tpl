#include <memory>
#include <vector>

#include "sql/tuple_id_list.h"
#include "sql/vector_operations/vector_operators.h"
#include "sql/vector_projection.h"
#include "util/test_harness.h"

namespace tpl::sql {

class VectorProjectionTest : public TplTest {
 public:
  VectorProjectionTest()
      : tinyint_col_("A", TinyIntType::Instance(false)),
        smallint_col_("B", SmallIntType::Instance(false)),
        int_col_("C", IntegerType::Instance(false)),
        bigint_col_("D", BigIntType::Instance(false)),
        float_col_("E", RealType::Instance(false)),
        double_col_("F", DoubleType::Instance(false)),
        date_col_("G", DateType::Instance(false)) {}

  const Schema::ColumnInfo *tinyint_col() const { return &tinyint_col_; }
  const Schema::ColumnInfo *smallint_col() const { return &smallint_col_; }
  const Schema::ColumnInfo *int_col() const { return &int_col_; }
  const Schema::ColumnInfo *bigint_col() const { return &bigint_col_; }
  const Schema::ColumnInfo *float_col() const { return &float_col_; }
  const Schema::ColumnInfo *double_col() const { return &double_col_; }
  const Schema::ColumnInfo *date_col() const { return &date_col_; }

 private:
  Schema::ColumnInfo tinyint_col_;
  Schema::ColumnInfo smallint_col_;
  Schema::ColumnInfo int_col_;
  Schema::ColumnInfo bigint_col_;
  Schema::ColumnInfo float_col_;
  Schema::ColumnInfo double_col_;
  Schema::ColumnInfo date_col_;
};

class VectorProjectionDeathTest : public VectorProjectionTest {};

TEST_F(VectorProjectionTest, Empty) {
  VectorProjection vector_projection;

  EXPECT_EQ(0u, vector_projection.GetColumnCount());
  EXPECT_EQ(0u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(0u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
  vector_projection.CheckIntegrity();
}

TEST_F(VectorProjectionTest, InitializeEmpty) {
  VectorProjection vector_projection;
  vector_projection.InitializeEmpty({smallint_col(), double_col()});

  EXPECT_EQ(2u, vector_projection.GetColumnCount());
  EXPECT_EQ(SmallIntType::Instance(false), vector_projection.GetColumnInfo(0)->sql_type);
  EXPECT_EQ(DoubleType::Instance(false), vector_projection.GetColumnInfo(1)->sql_type);
  EXPECT_EQ(0u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(0u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());

  for (uint32_t i = 0; i < vector_projection.GetColumnCount(); i++) {
    EXPECT_EQ(0u, vector_projection.GetColumn(i)->GetCount());
    EXPECT_EQ(nullptr, vector_projection.GetColumn(i)->GetFilteredTupleIdList());
  }

  vector_projection.CheckIntegrity();
}

TEST_F(VectorProjectionTest, Initialize) {
  VectorProjection vector_projection;
  vector_projection.Initialize({float_col(), int_col(), date_col()});

  EXPECT_EQ(3u, vector_projection.GetColumnCount());
  EXPECT_EQ(RealType::Instance(false), vector_projection.GetColumnInfo(0)->sql_type);
  EXPECT_EQ(IntegerType::Instance(false), vector_projection.GetColumnInfo(1)->sql_type);
  EXPECT_EQ(DateType::Instance(false), vector_projection.GetColumnInfo(2)->sql_type);
  EXPECT_EQ(0u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(0u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());

  for (uint32_t i = 0; i < vector_projection.GetColumnCount(); i++) {
    EXPECT_EQ(0u, vector_projection.GetColumn(i)->GetCount());
    EXPECT_EQ(nullptr, vector_projection.GetColumn(i)->GetFilteredTupleIdList());
  }

  vector_projection.CheckIntegrity();
}

TEST_F(VectorProjectionTest, Selection) {
  VectorProjection vector_projection;
  vector_projection.Initialize({bigint_col(), double_col()});
  vector_projection.Reset(20);

  // a = [i for i in range(0, 20, 3)] = [0, 3, 6, 9, 12, ...]
  // b = [123.45 for i in range(20)] = [123.45, 123.45, 123.45, ...]
  VectorOps::Generate(vector_projection.GetColumn(0), 0, 3);
  VectorOps::Fill(vector_projection.GetColumn(1), GenericValue::CreateDouble(123.45));

  EXPECT_EQ(20u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(20u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
  EXPECT_FLOAT_EQ(1.0, vector_projection.ComputeSelectivity());

  // Try to filter once
  {
    auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount());
    tid_list = {2, 3, 5, 7, 11, 13, 17, 19};
    vector_projection.SetFilteredSelections(tid_list);

    EXPECT_EQ(20u, vector_projection.GetTotalTupleCount());
    EXPECT_EQ(tid_list.GetTupleCount(), vector_projection.GetSelectedTupleCount());
    EXPECT_NE(nullptr, vector_projection.GetFilteredTupleIdList());
    EXPECT_FLOAT_EQ(tid_list.ComputeSelectivity(), vector_projection.ComputeSelectivity());
    for (uint32_t i = 0; i < tid_list.GetTupleCount(); i++) {
      auto tid = tid_list[i];
      EXPECT_EQ(GenericValue::CreateBigInt(tid * 3), vector_projection.GetColumn(0)->GetValue(i));
    }
  }

  // Filter again with a different selection
  {
    auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount());
    vector_projection.SetFilteredSelections(tid_list);

    EXPECT_EQ(20u, vector_projection.GetTotalTupleCount());
    EXPECT_EQ(tid_list.GetTupleCount(), vector_projection.GetSelectedTupleCount());
    EXPECT_NE(nullptr, vector_projection.GetFilteredTupleIdList());
    EXPECT_FLOAT_EQ(tid_list.ComputeSelectivity(), vector_projection.ComputeSelectivity());
  }

  vector_projection.CheckIntegrity();
}

TEST_F(VectorProjectionDeathTest, InvalidFilter) {
  VectorProjection vector_projection;
  vector_projection.Initialize({bigint_col(), double_col()});
  vector_projection.Reset(20);

  // Filtered TID list is too small
  {
    auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount() - 5);
    ASSERT_DEATH(vector_projection.SetFilteredSelections(tid_list), "capacity");
  }

  // Filtered TID list is too large
  {
    auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount() + 5);
    ASSERT_DEATH(vector_projection.SetFilteredSelections(tid_list), "capacity");
  }
}

TEST_F(VectorProjectionDeathTest, InvalidShape) {
  VectorProjection vector_projection;
  vector_projection.Initialize({bigint_col(), double_col()});
  vector_projection.Reset(20);

  // Vectors have different sizes
  {
    // Use second vector because first is used to determine projection size, in which case the
    // TID list capacity assertion will trip, not the all-vectors-have-same-size.
    vector_projection.GetColumn(1)->Resize(2);
    ASSERT_DEATH(vector_projection.CheckIntegrity(), "Vector size");
  }
}

TEST_F(VectorProjectionTest, Reset) {
  VectorProjection vector_projection;
  vector_projection.Initialize({tinyint_col()});
  vector_projection.Reset(20);

  auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount());
  tid_list = {7, 11, 13};
  vector_projection.SetFilteredSelections(tid_list);

  EXPECT_FALSE(vector_projection.IsEmpty());
  EXPECT_TRUE(vector_projection.IsFiltered());
  EXPECT_EQ(tid_list.GetTupleCount(), vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(20u, vector_projection.GetTotalTupleCount());
  EXPECT_NE(nullptr, vector_projection.GetFilteredTupleIdList());
  EXPECT_FLOAT_EQ(tid_list.ComputeSelectivity(), vector_projection.ComputeSelectivity());
  vector_projection.CheckIntegrity();

  vector_projection.Reset(0);

  EXPECT_TRUE(vector_projection.IsEmpty());
  EXPECT_FALSE(vector_projection.IsFiltered());
  EXPECT_EQ(0u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(0u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
  EXPECT_FLOAT_EQ(0.0, vector_projection.ComputeSelectivity());

  vector_projection.Reset(40);

  EXPECT_FALSE(vector_projection.IsEmpty());
  EXPECT_FALSE(vector_projection.IsFiltered());
  EXPECT_EQ(40u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(40u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
  EXPECT_FLOAT_EQ(1.0, vector_projection.ComputeSelectivity());

  vector_projection.CheckIntegrity();
}

}  // namespace tpl::sql
