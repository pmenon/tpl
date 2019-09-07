#include <vector>

#include "sql_test.h"

#include "sql/constant_vector.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

class VectorBooleanLogicTest : public TplTest {};

TEST_F(VectorBooleanLogicTest, BooleanLogic) {
  // a = [false, false, true, true]
  // b = [false, true, false, true]
  // c = false
  // d = NULL(boolean)
  auto a = MakeBooleanVector({false, false, true, true}, {false, false, false, false});
  auto b = MakeBooleanVector({false, true, false, true}, {false, false, false, false});
  auto c = ConstantVector(GenericValue::CreateBoolean(false));
  auto d = ConstantVector(GenericValue::CreateNull(c.type_id()));
  auto result = MakeBooleanVector();

  // a && b = [false, false, false, true]
  VectorOps::And(*a, *b, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_FALSE(result->null_mask().Any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(3));

  // a || b = [false, true, true, true]
  VectorOps::Or(*a, *b, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_FALSE(result->null_mask().Any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(3));

  // !a = [true, true, false, false]
  VectorOps::Not(*a, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_FALSE(result->null_mask().Any());
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(3));

  // aa = [false, NULL, true, true]
  auto aa = MakeBooleanVector();
  a->CopyTo(aa.get());
  aa->SetValue(1, GenericValue::CreateNull(TypeId::Boolean));

  // aa && b = [false, NULL, false, true]
  VectorOps::And(*aa, *b, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_TRUE(result->null_mask().Any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateNull(TypeId::Boolean), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(3));

  // a && c = [false, false, false, false]
  VectorOps::And(*a, c, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_FALSE(result->null_mask().Any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(3));

  // c && a = [false, false, false, false]
  VectorOps::And(c, *a, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_FALSE(result->null_mask().Any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(3));

  // d && a = [false, false, NULL, NULL]
  VectorOps::And(d, *a, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_TRUE(result->null_mask().Any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(1));
  EXPECT_EQ(d.GetValue(0), result->GetValue(2));
  EXPECT_EQ(d.GetValue(0), result->GetValue(3));

  // d || b = [NULL, true, NULL, true]
  VectorOps::Or(d, *b, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_TRUE(result->null_mask().Any());
  EXPECT_EQ(d.GetValue(0), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(1));
  EXPECT_EQ(d.GetValue(0), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(3));
}

TEST_F(VectorBooleanLogicTest, FilteredBooleanLogic) {
  // a = [NULL, false, true, true], b = [false, true, false, true]
  auto a = MakeBooleanVector({false, false, true, true}, {true, true, false, false});
  auto b = MakeBooleanVector({false, true, false, true}, {false, false, false, false});
  auto result = MakeBooleanVector();
  std::vector<uint16_t> sel = {0, 1, 3};

  // Set selection vector for both a and b
  a->SetSelectionVector(sel.data(), sel.size());
  b->SetSelectionVector(sel.data(), sel.size());

  // result = a && b
  VectorOps::And(*a, *b, result.get());
  EXPECT_EQ(3u, result->count());
  EXPECT_NE(nullptr, result->selection_vector());
  EXPECT_TRUE(result->null_mask().Any());

  // result[0] = NULL && false = false
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));

  // result[1] = NULL && true = NULL
  EXPECT_EQ(GenericValue::CreateNull(TypeId::Boolean), result->GetValue(1));

  // result[2] = true && true = true
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(2));
}

TEST_F(VectorBooleanLogicTest, NullChecking) {
  auto vec = MakeFloatVector({1.0, 0.0, 1.0, 0.0}, {false, true, false, true});
  auto result = MakeBooleanVector();

  // IS NULL vec, only 1 and 3
  VectorOps::IsNull(*vec, result.get());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(3));

  // IS NOT NULL vec, only 0 and 2
  VectorOps::IsNotNull(*vec, result.get());
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(3));
}

TEST_F(VectorBooleanLogicTest, AnyOrAll) {
  // vec = [false, false, false, false]
  auto vec = MakeBooleanVector({false, false, false, false}, {false, false, false, false});

  EXPECT_FALSE(VectorOps::AnyTrue(*vec));
  EXPECT_FALSE(VectorOps::AllTrue(*vec));

  // vec = [false, false, false, NULL]
  vec->SetValue(3, GenericValue::CreateNull(TypeId::Boolean));

  EXPECT_FALSE(VectorOps::AnyTrue(*vec));
  EXPECT_FALSE(VectorOps::AllTrue(*vec));

  // vec = [false, false, false, true]
  vec->SetValue(3, GenericValue::CreateBoolean(true));

  EXPECT_TRUE(VectorOps::AnyTrue(*vec));
  EXPECT_FALSE(VectorOps::AllTrue(*vec));

  // vec = [true, true, true, true]
  vec = MakeBooleanVector({true, true, true, true}, {false, false, false, false});

  EXPECT_TRUE(VectorOps::AnyTrue(*vec));
  EXPECT_TRUE(VectorOps::AllTrue(*vec));
}

}  // namespace tpl::sql
