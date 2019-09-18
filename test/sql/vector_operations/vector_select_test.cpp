#include <vector>

#include "common/exception.h"
#include "sql/constant_vector.h"
#include "sql/tuple_id_list.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/sql_test_harness.h"

namespace tpl::sql {

class VectorSelectTest : public TplTest {};

TEST_F(VectorSelectTest, MismatchedInputTypes) {
  auto a = MakeTinyIntVector(2);
  auto b = MakeBigIntVector(2);
  auto result = TupleIdList(a->num_elements());
  result.AddAll();
  EXPECT_THROW(VectorOps::SelectEqual(*a, *b, &result), TypeMismatchException);
}

TEST_F(VectorSelectTest, MismatchedSizes) {
  auto a = MakeTinyIntVector(54);
  auto b = MakeBigIntVector(19);
  auto result = TupleIdList(a->num_elements());
  result.AddAll();
  EXPECT_THROW(VectorOps::SelectEqual(*a, *b, &result), Exception);
}

TEST_F(VectorSelectTest, MismatchedCounts) {
  auto a = MakeTinyIntVector(10);
  auto b = MakeBigIntVector(10);

  sel_t sel_1[] = {0, 1, 2};
  sel_t sel_2[] = {9, 8};

  a->SetSelectionVector(sel_1, 3);
  b->SetSelectionVector(sel_2, 2);

  auto result = TupleIdList(a->num_elements());
  result.AddAll();

  EXPECT_THROW(VectorOps::SelectEqual(*a, *b, &result), Exception);
}

TEST_F(VectorSelectTest, InvalidTIDListSize) {
  auto a = MakeTinyIntVector(10);
  auto b = MakeBigIntVector(10);

  auto result = TupleIdList(1);
  result.AddAll();

  EXPECT_THROW(VectorOps::SelectEqual(*a, *b, &result), Exception);
}

TEST_F(VectorSelectTest, BasicSelect) {
  // a = [NULL, 1, 6, NULL, 4, 5]
  // b = [0, NULL, 4, NULL, 5, 5]
  auto a = MakeTinyIntVector({0, 1, 6, 3, 4, 5}, {true, false, false, true, false, false});
  auto b = MakeTinyIntVector({0, 1, 4, 3, 5, 5}, {false, true, false, true, false, false});
  auto _2 = ConstantVector(GenericValue::CreateTinyInt(2));

  for (auto type_id : {TypeId::TinyInt, TypeId::SmallInt, TypeId::Integer, TypeId::BigInt,
                       TypeId::Float, TypeId::Double}) {
    a->Cast(type_id);
    b->Cast(type_id);
    _2.Cast(type_id);

    TupleIdList input_list(a->num_elements());
    input_list.AddAll();

    // a < 2
    VectorOps::SelectLessThan(*a, _2, &input_list);
    EXPECT_EQ(1u, input_list.GetTupleCount());
    EXPECT_EQ(1u, input_list[0]);

    input_list.AddAll();

    // 2 < a
    VectorOps::SelectLessThan(_2, *a, &input_list);
    EXPECT_EQ(3u, input_list.GetTupleCount());
    EXPECT_EQ(2u, input_list[0]);
    EXPECT_EQ(4u, input_list[1]);
    EXPECT_EQ(5u, input_list[2]);

    input_list.AddAll();

    // 2 == a
    VectorOps::SelectEqual(_2, *a, &input_list);
    EXPECT_TRUE(input_list.IsEmpty());

    input_list.AddAll();

    // a != b = [2, 4]
    VectorOps::SelectNotEqual(*a, *b, &input_list);
    EXPECT_EQ(2u, input_list.GetTupleCount());
    EXPECT_EQ(2u, input_list[0]);
    EXPECT_EQ(4u, input_list[1]);

    input_list.AddAll();

    // b == a = [5]
    VectorOps::SelectEqual(*b, *a, &input_list);
    EXPECT_EQ(1u, input_list.GetTupleCount());
    EXPECT_EQ(5u, input_list[0]);

    input_list.AddAll();

    // a < b = [4]
    VectorOps::SelectLessThan(*a, *b, &input_list);
    EXPECT_EQ(1u, input_list.GetTupleCount());
    EXPECT_EQ(4u, input_list[0]);

    input_list.AddAll();

    // a <= b = [4, 5]
    VectorOps::SelectLessThanEqual(*a, *b, &input_list);
    EXPECT_EQ(2, input_list.GetTupleCount());
    EXPECT_EQ(4u, input_list[0]);
    EXPECT_EQ(5u, input_list[1]);

    input_list.AddAll();

    // a > b = [2]
    VectorOps::SelectGreaterThan(*a, *b, &input_list);
    EXPECT_EQ(1, input_list.GetTupleCount());
    EXPECT_EQ(2u, input_list[0]);

    input_list.AddAll();

    // a >= b = [2]
    VectorOps::SelectGreaterThanEqual(*a, *b, &input_list);
    EXPECT_EQ(2, input_list.GetTupleCount());
    EXPECT_EQ(2u, input_list[0]);
    EXPECT_EQ(5u, input_list[1]);
  }
}

TEST_F(VectorSelectTest, SelectNullConstant) {
  // a = [0, 1, NULL, NULL, 4, 5]
  auto a = MakeIntegerVector({0, 1, 2, 3, 4, 5}, {false, false, true, true, false, false});
  auto null_constant = ConstantVector(GenericValue::CreateNull(a->type_id()));

#define NULL_TEST(OP)                                \
  /* a <OP> NULL */                                  \
  {                                                  \
    TupleIdList list(a->num_elements());             \
    list.AddAll();                                   \
    VectorOps::Select##OP(*a, null_constant, &list); \
    EXPECT_TRUE(list.IsEmpty());                     \
  }                                                  \
  /* NULL <OP> a */                                  \
  {                                                  \
    TupleIdList list(a->num_elements());             \
    list.AddAll();                                   \
    VectorOps::Select##OP(*a, null_constant, &list); \
    EXPECT_TRUE(list.IsEmpty());                     \
  }

  NULL_TEST(Equal)
  NULL_TEST(GreaterThan)
  NULL_TEST(GreaterThanEqual)
  NULL_TEST(LessThan)
  NULL_TEST(LessThanEqual)
  NULL_TEST(NotEqual)

#undef NULL_TEST
}

TEST_F(VectorSelectTest, IsNullAndIsNotNull) {
  auto vec = MakeFloatVector({1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0},
                             {false, true, false, true, true, false, false});
  auto tid_list = TupleIdList(vec->num_elements());

  // Try first with a full TID list

  // IS NULL(vec) = [1, 3, 4]
  tid_list.AddAll();
  VectorOps::IsNull(*vec, &tid_list);
  EXPECT_EQ(3u, tid_list.GetTupleCount());
  EXPECT_EQ(1u, tid_list[0]);
  EXPECT_EQ(3u, tid_list[1]);
  EXPECT_EQ(4u, tid_list[2]);

  // IS_NOT_NULL(vec) = [0, 2, 5, 6]
  tid_list.AddAll();
  VectorOps::IsNotNull(*vec, &tid_list);
  EXPECT_EQ(4u, tid_list.GetTupleCount());
  EXPECT_EQ(0u, tid_list[0]);
  EXPECT_EQ(2u, tid_list[1]);
  EXPECT_EQ(5u, tid_list[2]);
  EXPECT_EQ(6u, tid_list[3]);

  // Try with a partial input list

  tid_list.Clear();
  tid_list.Add(1);
  tid_list.Add(4);
  VectorOps::IsNull(*vec, &tid_list);
  EXPECT_EQ(2u, tid_list.GetTupleCount());
  EXPECT_EQ(1u, tid_list[0]);
  EXPECT_EQ(4u, tid_list[1]);

  tid_list.Clear();
  tid_list.AddRange(2, 5);
  VectorOps::IsNotNull(*vec, &tid_list);
  EXPECT_EQ(1u, tid_list.GetTupleCount());
  EXPECT_EQ(2u, tid_list[0]);
}

}  // namespace tpl::sql
