
#include <memory>
#include <vector>

#include "sql/schema.h"
#include "sql/vector_filter_executor.h"
#include "sql/vector_operations/vector_operators.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/test_harness.h"

namespace tpl::sql {

class VectorFilterExecutorTest : public TplTest {
 public:
  enum Col {
    A = 0,
    B = 1,
    C = 2,
    D = 3,
    E = 4,
    F = 5,
  };

  VectorFilterExecutorTest() {
    cols_.emplace_back("colA", BooleanType::InstanceNonNullable());
    cols_.emplace_back("colB", TinyIntType::InstanceNonNullable());
    cols_.emplace_back("colC", SmallIntType::InstanceNonNullable());
    cols_.emplace_back("colD", RealType::InstanceNonNullable());
    cols_.emplace_back("colE", VarcharType::InstanceNonNullable(40));
    cols_.emplace_back("colF", SmallIntType::InstanceNonNullable());
  }

  std::vector<const Schema::ColumnInfo *> Cols() {
    std::vector<const Schema::ColumnInfo *> result;
    for (const auto &col : cols_) {
      result.push_back(&col);
    }
    return result;
  }

  // clang-format off
  // Create a test projection with five columns:
  // cola(bool)     = [f,f,f,f,f,t,t,t,t,t]
  // colb(tinyint)  = [0,1,2,0,1,2,0,1,2,0]
  // colc(smallint) = [4,5,6,7,8,9,10,11,12,13]
  // cold(real)     = [1.1,2,2,3.3,4.4,5.5,6.6,7.7,8.8,9.9,10.1]
  // cole(varchar)  = ['one','two','three','four','five','six','seven','eight','nine','ten']
  // colf(smallint) = [1,5,2,7,3,9,4,11,5,13]
  // clang-format on
  std::unique_ptr<VectorProjection> CreateTestVectorProj() {
    auto vp = std::make_unique<VectorProjection>();
    vp->Initialize(Cols());
    vp->SetTupleCount(10);

    static const char *nums[] = {"one", "two",   "three", "four", "five",
                                 "six", "seven", "eight", "nine", "ten"};
    for (uint64_t i = 0; i < vp->GetTupleCount(); i++) {
      vp->GetColumn(Col::A)->SetValue(i, GenericValue::CreateBoolean(i >= 5));
      vp->GetColumn(Col::B)->SetValue(i, GenericValue::CreateTinyInt(i % 3));
      vp->GetColumn(Col::C)->SetValue(i, GenericValue::CreateSmallInt(i + 4));
      vp->GetColumn(Col::D)->SetValue(
          i, GenericValue::CreateReal((i + 1) + static_cast<float>(i + 1) / 10.0));
      vp->GetColumn(Col::E)->SetValue(i, GenericValue::CreateVarchar(nums[i]));
      vp->GetColumn(Col::F)->SetValue(i, GenericValue::CreateSmallInt(i % 2 == 0 ? i + 1 : i + 4));
    }

    return vp;
  }

 private:
  std::vector<Schema::ColumnInfo> cols_;
};

TEST_F(VectorFilterExecutorTest, ColumnWithConstant) {
  auto check_loop = [](VectorProjection *vp, uint32_t expected_size, auto cb) {
    EXPECT_EQ(expected_size, vp->GetTupleCount());
    for (VectorProjectionIterator iter(vp); iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      cb(&iter);
    }
  };

  // cole = 'ten'
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectEqVal(Col::E, GenericValue::CreateVarchar("ten"));
    filter.Finish();

    check_loop(vp.get(), 1, [](VectorProjectionIterator *iter) {
      auto cole = *iter->GetValue<char *, false>(Col::E, nullptr);
      EXPECT_STREQ("ten", cole);
    });
  }

  // colb > 1
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectGtVal(Col::B, GenericValue::CreateTinyInt(1));
    filter.Finish();

    check_loop(vp.get(), 3, [](VectorProjectionIterator *iter) {
      auto colb = *iter->GetValue<int8_t, false>(Col::B, nullptr);
      EXPECT_GT(colb, 1);
    });
  }

  // colc >= 10
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectGeVal(Col::C, GenericValue::CreateSmallInt(10));
    filter.Finish();

    check_loop(vp.get(), 4, [](VectorProjectionIterator *iter) {
      auto colb = *iter->GetValue<int16_t, false>(Col::C, nullptr);
      EXPECT_GE(colb, 10);
    });
  }

  // cold < 4.0
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectLtVal(Col::D, GenericValue::CreateReal(4.0));
    filter.Finish();

    check_loop(vp.get(), 3, [](VectorProjectionIterator *iter) {
      auto cold = *iter->GetValue<float, false>(Col::D, nullptr);
      EXPECT_LT(cold, 4.0);
    });
  }

  // colb <= 4
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectLeVal(Col::B, GenericValue::CreateTinyInt(4));
    filter.Finish();

    check_loop(vp.get(), vp->GetTupleCount(), [](VectorProjectionIterator *iter) {
      auto colb = *iter->GetValue<int8_t, false>(Col::B, nullptr);
      EXPECT_LE(colb, 4);
    });
  }

  // cola != t
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectNeVal(Col::A, GenericValue::CreateBoolean(true));
    filter.Finish();

    check_loop(vp.get(), 5, [](VectorProjectionIterator *iter) {
      auto cola = *iter->GetValue<bool, false>(Col::A, nullptr);
      EXPECT_NE(cola, true);
    });
  }
}

TEST_F(VectorFilterExecutorTest, Conjunction_Between) {
  auto vp = CreateTestVectorProj();

  // colc >= 8 AND colc <= 10
  int16_t colc_lo = 8, colc_hi = 10;
  VectorFilterExecutor filter(vp.get());
  filter.SelectGeVal(Col::C, GenericValue::CreateSmallInt(colc_lo));
  filter.SelectLeVal(Col::C, GenericValue::CreateSmallInt(colc_hi));
  filter.Finish();

  EXPECT_EQ(3u, vp->GetTupleCount());
  for (VectorProjectionIterator iter(vp.get()); iter.HasNextFiltered(); iter.AdvanceFiltered()) {
    auto colc = *iter.GetValue<int16_t, false>(Col::C, nullptr);
    EXPECT_TRUE(colc >= colc_lo && colc <= colc_hi);
  }
}

TEST_F(VectorFilterExecutorTest, ColumnWithColumn) {
  auto check_loop = [](VectorProjection *vp, uint32_t expected_size, auto cb) {
    EXPECT_EQ(expected_size, vp->GetTupleCount());
    for (VectorProjectionIterator iter(vp); iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      cb(&iter);
    }
  };

  // colc = colf
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectEq(Col::C, Col::F);
    filter.Finish();
    check_loop(vp.get(), 5, [](VectorProjectionIterator *iter) {
      auto colc = *iter->GetValue<int16_t, false>(Col::C, nullptr);
      auto colf = *iter->GetValue<int16_t, false>(Col::F, nullptr);
      EXPECT_EQ(colc, colf);
    });
  }

  // colc >= colf
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectGe(Col::C, Col::F);
    filter.Finish();
    check_loop(vp.get(), 10, [](VectorProjectionIterator *iter) {
      auto colc = *iter->GetValue<int16_t, false>(Col::C, nullptr);
      auto colf = *iter->GetValue<int16_t, false>(Col::F, nullptr);
      EXPECT_GE(colc, colf);
    });
  }

  // colc > colf
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectGt(Col::C, Col::F);
    filter.Finish();
    check_loop(vp.get(), 5, [](VectorProjectionIterator *iter) {
      auto colc = *iter->GetValue<int16_t, false>(Col::C, nullptr);
      auto colf = *iter->GetValue<int16_t, false>(Col::F, nullptr);
      EXPECT_GT(colc, colf);
    });
  }

  // colc < colf
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectLt(Col::C, Col::F);
    filter.Finish();
    check_loop(vp.get(), 0, [](VectorProjectionIterator *iter) { FAIL(); });
  }

  // colc <= colf
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectLe(Col::C, Col::F);
    filter.Finish();
    check_loop(vp.get(), 5, [](VectorProjectionIterator *iter) {
      auto colc = *iter->GetValue<int16_t, false>(Col::C, nullptr);
      auto colf = *iter->GetValue<int16_t, false>(Col::F, nullptr);
      EXPECT_LE(colc, colf);
    });
  }

  // colc <> colf
  {
    auto vp = CreateTestVectorProj();
    VectorFilterExecutor filter(vp.get());
    filter.SelectNe(Col::C, Col::F);
    filter.Finish();
    check_loop(vp.get(), 5, [](VectorProjectionIterator *iter) {
      auto colc = *iter->GetValue<int16_t, false>(Col::C, nullptr);
      auto colf = *iter->GetValue<int16_t, false>(Col::F, nullptr);
      EXPECT_NE(colc, colf);
    });
  }
}

}  // namespace tpl::sql