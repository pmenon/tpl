#include "sql/vector_projection.h"
#include "util/sql_test_harness.h"
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

TEST_F(VectorProjectionTest, Empty) {
  VectorProjection vp;

  EXPECT_EQ(0u, vp.GetNumColumns());
  EXPECT_EQ(0u, vp.GetTupleCount());
  EXPECT_EQ(nullptr, vp.GetSelectionVector());
  vp.CheckIntegrity();
}

TEST_F(VectorProjectionTest, InitializeEmpty) {
  VectorProjection vp;
  vp.InitializeEmpty({smallint_col(), double_col()});

  EXPECT_EQ(2u, vp.GetNumColumns());
  EXPECT_EQ(SmallIntType::Instance(false), vp.GetColumnInfo(0)->sql_type);
  EXPECT_EQ(DoubleType::Instance(false), vp.GetColumnInfo(1)->sql_type);
  EXPECT_EQ(0u, vp.GetTupleCount());
  EXPECT_EQ(nullptr, vp.GetSelectionVector());

  for (uint32_t i = 0; i < vp.GetNumColumns(); i++) {
    EXPECT_EQ(0u, vp.GetColumn(i)->count());
    EXPECT_EQ(nullptr, vp.GetColumn(i)->selection_vector());
  }

  vp.CheckIntegrity();
}

TEST_F(VectorProjectionTest, Initialize) {
  VectorProjection vp;
  vp.Initialize({float_col(), int_col(), date_col()});

  EXPECT_EQ(3u, vp.GetNumColumns());
  EXPECT_EQ(RealType::Instance(false), vp.GetColumnInfo(0)->sql_type);
  EXPECT_EQ(IntegerType::Instance(false), vp.GetColumnInfo(1)->sql_type);
  EXPECT_EQ(DateType::Instance(false), vp.GetColumnInfo(2)->sql_type);
  EXPECT_EQ(0u, vp.GetTupleCount());
  EXPECT_EQ(nullptr, vp.GetSelectionVector());

  for (uint32_t i = 0; i < vp.GetNumColumns(); i++) {
    EXPECT_EQ(0u, vp.GetColumn(i)->count());
    EXPECT_EQ(nullptr, vp.GetColumn(i)->selection_vector());
  }

  vp.CheckIntegrity();
}

}  // namespace tpl::sql
