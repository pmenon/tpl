#include "common/exception.h"
#include "sql/constant_vector.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/sql_test_harness.h"
#include "util/test_harness.h"

namespace tpl::sql {

class VectorArithmeticTest : public TplTest {};

TEST_F(VectorArithmeticTest, InvalidVectorShapes) {
  auto a = MakeIntegerVector(10);
  auto b = MakeIntegerVector(20);
  auto result = Vector(TypeId::SmallInt, true, false);

  // Check simple invalid input sizes
  EXPECT_THROW(VectorOps::Add(*a, *b, &result), tpl::Exception);

  a->Resize(10);
  b->Resize(10);

  // Check invalid types
  EXPECT_THROW(VectorOps::Add(*a, *b, &result), tpl::TypeMismatchException);
}

TEST_F(VectorArithmeticTest, Addition) {
  auto a = MakeIntegerVector(100);
  auto b = MakeIntegerVector(100);
  auto result = Vector(TypeId::Integer, true, false);

  // a = [10,10,10,10,10,...]
  // b = [0,2,4,6,8,10,...]
  VectorOps::Fill(a.get(), GenericValue::CreateInteger(10));
  VectorOps::Generate(b.get(), 0, 2);

  {
    // No-nulls, no filter, vector + vector
    // result = a + b = [10,12,14,16,18,...]
    VectorOps::Add(*a, *b, &result);

    EXPECT_EQ(a->GetSize(), result.GetSize());
    EXPECT_EQ(a->GetCount(), result.GetCount());
    EXPECT_EQ(nullptr, result.GetFilteredTupleIdList());
    EXPECT_FALSE(result.GetNullMask().Any());

    auto result_data = reinterpret_cast<int32_t *>(result.GetData());
    for (uint64_t i = 0; i < result.GetCount(); i++) {
      EXPECT_EQ(i * 2 + 10, result_data[i]);
    }
  }

  {
    // No-nulls, filter, constant + vector
    auto tid_list = TupleIdList(b->GetSize());
    tid_list = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90};
    b->SetFilteredTupleIdList(&tid_list, tid_list.GetTupleCount());

    // result = 13 + b = [13,15,17,19,...]
    VectorOps::Add(ConstantVector(GenericValue::CreateInteger(13)), *b, &result);

    EXPECT_EQ(b->GetSize(), result.GetSize());
    EXPECT_EQ(b->GetCount(), result.GetCount());
    EXPECT_EQ(b->GetFilteredTupleIdList(), result.GetFilteredTupleIdList());
    EXPECT_FALSE(result.GetNullMask().Any());

    for (uint64_t i = 0; i < result.GetCount(); i++) {
      EXPECT_EQ(GenericValue::CreateInteger(tid_list[i] * 2 + 13), result.GetValue(i));
    }

    b->Resize(100);
  }

  {
    // Null, filter, vector + constant
    VectorOps::Add(*a, ConstantVector(GenericValue::CreateNull(TypeId::Integer)), &result);

    EXPECT_EQ(a->GetSize(), result.GetSize());
    EXPECT_EQ(a->GetCount(), result.GetCount());
    EXPECT_EQ(a->GetFilteredTupleIdList(), result.GetFilteredTupleIdList());
    EXPECT_TRUE(result.GetNullMask().All());
  }
}

TEST_F(VectorArithmeticTest, DivMod) {
  auto a = MakeSmallIntVector(100);
  auto b = MakeSmallIntVector(100);
  auto result = Vector(TypeId::SmallInt, true, false);

  // a = [0,2,4,6,8,10,...]
  // b = [0,4,8,12,16,20,...]
  VectorOps::Generate(a.get(), 0, 2);
  VectorOps::Generate(b.get(), 0, 4);

  {
    // Nulls, zeros, no filter, vector + vector
    b->SetNull(2, true);
    b->SetNull(9, true);
    a->SetValue(1, GenericValue::CreateSmallInt(0));
    a->SetValue(11, GenericValue::CreateSmallInt(0));
    VectorOps::Divide(*b, *a, &result);

    EXPECT_EQ(a->GetSize(), result.GetSize());
    EXPECT_EQ(a->GetCount(), result.GetCount());
    EXPECT_EQ(nullptr, result.GetFilteredTupleIdList());
    EXPECT_TRUE(result.IsNull(1));
    EXPECT_TRUE(result.IsNull(2));
    EXPECT_TRUE(result.IsNull(9));
    EXPECT_TRUE(result.IsNull(11));

    for (uint64_t i = 0; i < result.GetCount(); i++) {
      if (!result.IsNull(i)) {
        EXPECT_EQ(GenericValue::CreateSmallInt(2), result.GetValue(i));
      }
    }
  }
}

}  // namespace tpl::sql