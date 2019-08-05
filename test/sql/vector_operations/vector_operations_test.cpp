#include <vector>

#include "sql_test.h"  // NOLINT

#include "sql/constant_vector.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/fast_rand.h"

namespace tpl::sql::test {

class VectorOperationsTest : public TplTest {};

TEST_F(VectorOperationsTest, Generate) {
  const u32 num_elems = 50;

  // Generate odd sequence of numbers starting at 1 inclusive. In other words,
  // generate the values [2*i+1 for i in range(0,50)]
#define CHECK_SIMPLE_GENERATE(TYPE)                          \
  {                                                          \
    auto vec = Make##TYPE##Vector(num_elems);                \
    VectorOps::Generate(vec.get(), 1, 2);                    \
    for (u64 i = 0; i < vec->count(); i++) {                 \
      auto val = vec->GetValue(i);                           \
      EXPECT_EQ(GenericValue::Create##TYPE(2 * i + 1), val); \
    }                                                        \
  }

  CHECK_SIMPLE_GENERATE(TinyInt)
  CHECK_SIMPLE_GENERATE(SmallInt)
  CHECK_SIMPLE_GENERATE(Integer)
  CHECK_SIMPLE_GENERATE(BigInt)
  CHECK_SIMPLE_GENERATE(Float)
  CHECK_SIMPLE_GENERATE(Double)
#undef CHECK_SIMPLE_GENERATE
}

TEST_F(VectorOperationsTest, Fill) {
  // Fill a vector with the given type with the given value of that type
#define CHECK_SIMPLE_FILL(TYPE, FILL_VALUE)                             \
  {                                                                     \
    auto vec = Make##TYPE##Vector(10);                                  \
    VectorOps::Fill(vec.get(), GenericValue::Create##TYPE(FILL_VALUE)); \
    for (u64 i = 0; i < vec->count(); i++) {                            \
      auto val = vec->GetValue(i);                                      \
      EXPECT_FALSE(val.is_null());                                      \
      EXPECT_EQ(GenericValue::Create##TYPE(FILL_VALUE), val);           \
    }                                                                   \
  }

  CHECK_SIMPLE_FILL(Boolean, true);
  CHECK_SIMPLE_FILL(TinyInt, i64(-24));
  CHECK_SIMPLE_FILL(SmallInt, i64(47));
  CHECK_SIMPLE_FILL(Integer, i64(1234));
  CHECK_SIMPLE_FILL(BigInt, i64(-24987));
  CHECK_SIMPLE_FILL(Float, f64(-3.10));
  CHECK_SIMPLE_FILL(Double, f64(-3.14));
  CHECK_SIMPLE_FILL(Varchar, "P-Money In The Bank");
#undef CHECK_SIMPLE_FILL
}

TEST_F(VectorOperationsTest, CompareNumeric) {
  // Input vector: [0,1,2,3,4,5]
  Vector vec(TypeId::BigInt, 6, false);
  VectorOps::Generate(&vec, 0, 1);

  for (auto type_id : {TypeId::TinyInt, TypeId::SmallInt, TypeId::Integer,
                       TypeId::BigInt, TypeId::Float, TypeId::Double}) {
    vec.Cast(type_id);

    // Try to find
    ConstantVector _4(GenericValue::CreateBigInt(4).CastTo(type_id));
    Vector result(TypeId::Boolean, true, true);

    // Only index 4 == 4
    {
      auto check = [&]() {
        EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(0));
        EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(1));
        EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(2));
        EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(3));
        EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(4));
        EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(5));
      };
      VectorOps::Equal(vec, _4, &result);
      check();
      VectorOps::Equal(_4, vec, &result);
      check();
    }

    // Only index 5 > 4
    {
      VectorOps::GreaterThan(vec, _4, &result);
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(0));
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(1));
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(2));
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(3));
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(4));
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(5));
    }

    // Indexes 4-5 are >= 4
    {
      VectorOps::GreaterThanEqual(vec, _4, &result);
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(0));
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(1));
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(2));
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(3));
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(4));
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(5));
    }

    // Indexes 0-3 are < 4
    {
      VectorOps::LessThan(vec, _4, &result);
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(0));
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(1));
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(2));
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(3));
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(4));
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(5));
    }

    // Indexes 0-4 are < 4
    {
      VectorOps::LessThanEqual(vec, _4, &result);
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(0));
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(1));
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(2));
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(3));
      EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(4));
      EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(5));
    }

    // Indexes 0,1,2,3,5 are != 4
    {
      auto check = [&]() {
        EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(0));
        EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(1));
        EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(2));
        EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(3));
        EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(4));
        EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(5));
      };
      VectorOps::NotEqual(vec, _4, &result);
      check();
      VectorOps::NotEqual(_4, vec, &result);
      check();
    }
  }
}

TEST_F(VectorOperationsTest, CompareStrings) {
  auto a = MakeVarcharVector({"first", "second", nullptr, "fourth"},
                             {false, false, true, false});
  auto b = MakeVarcharVector({nullptr, "second", nullptr, "baka not nice"},
                             {true, false, true, false});
  auto result = MakeBooleanVector();

  VectorOps::Equal(*a, *b, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_TRUE(result->IsNull(0));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(1));
  EXPECT_TRUE(result->IsNull(2));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(3));
}

TEST_F(VectorOperationsTest, CompareWithNulls) {
  auto input = MakeBigIntVector({0, 1, 2, 3}, {false, false, false, false});
  auto null = ConstantVector(GenericValue::CreateNull(TypeId::BigInt));
  auto result = MakeBooleanVector();

  VectorOps::Equal(*input, null, result.get());
  EXPECT_TRUE(result->IsNull(0));
  EXPECT_TRUE(result->IsNull(1));
  EXPECT_TRUE(result->IsNull(2));
  EXPECT_TRUE(result->IsNull(3));
}

TEST_F(VectorOperationsTest, NullChecking) {
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

TEST_F(VectorOperationsTest, AnyOrAllTrue) {
  auto vec = MakeBooleanVector({false, false, false, false},
                               {false, false, false, false});

  EXPECT_FALSE(VectorOps::AnyTrue(*vec));
  EXPECT_FALSE(VectorOps::AllTrue(*vec));

  vec->SetValue(3, GenericValue::CreateNull(TypeId::Boolean));

  EXPECT_FALSE(VectorOps::AnyTrue(*vec));
  EXPECT_FALSE(VectorOps::AllTrue(*vec));

  vec->SetValue(3, GenericValue::CreateBoolean(true));

  EXPECT_TRUE(VectorOps::AnyTrue(*vec));
  EXPECT_FALSE(VectorOps::AllTrue(*vec));

  vec =
      MakeBooleanVector({true, true, true, true}, {false, false, false, false});

  EXPECT_TRUE(VectorOps::AnyTrue(*vec));
  EXPECT_TRUE(VectorOps::AllTrue(*vec));
}

TEST_F(VectorOperationsTest, BooleanLogic) {
  // a = [false, false, true, true]
  // b = [false, true, false, true]
  // c = false
  auto a = MakeBooleanVector({false, false, true, true},
                             {false, false, false, false});
  auto b = MakeBooleanVector({false, true, false, true},
                             {false, false, false, false});
  auto c = ConstantVector(GenericValue::CreateBoolean(false));
  auto result = MakeBooleanVector();

  // a && b = [false, false, false, true]
  VectorOps::And(*a, *b, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_FALSE(result->null_mask().any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(3));

  // a || b = [false, true, true, true]
  VectorOps::Or(*a, *b, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_FALSE(result->null_mask().any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(3));

  // !a = [true, true, false, false]
  VectorOps::Not(*a, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_FALSE(result->null_mask().any());
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
  EXPECT_TRUE(result->null_mask().any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateNull(TypeId::Boolean), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(3));

  // a && c = [false, false, false, false]
  VectorOps::And(*a, c, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_FALSE(result->null_mask().any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(3));

  // c && a = [false, false, false, false]
  VectorOps::And(c, *a, result.get());
  EXPECT_EQ(4u, result->count());
  EXPECT_EQ(nullptr, result->selection_vector());
  EXPECT_FALSE(result->null_mask().any());
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(1));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(2));
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(3));
}

TEST_F(VectorOperationsTest, FilteredBooleanLogic) {
  // a = [NULL, false, true, true], b = [false, true, false, true]
  auto a =
      MakeBooleanVector({false, false, true, true}, {true, true, false, false});
  auto b = MakeBooleanVector({false, true, false, true},
                             {false, false, false, false});
  auto result = MakeBooleanVector();
  auto selected_tids = MakeTupleIdList({0, 1, 3});
  std::vector<u16> sel = {0, 1, 3};

  // Set selection vector for both a and b
  a->SetSelectionVector(sel.data(), selected_tids->GetNumTuples());
  b->SetSelectionVector(sel.data(), selected_tids->GetNumTuples());

  // result = a && b
  VectorOps::And(*a, *b, result.get());
  EXPECT_EQ(3u, result->count());
  EXPECT_NE(nullptr, result->selection_vector());
  EXPECT_TRUE(result->null_mask().any());

  // result[0] = NULL && false = false
  EXPECT_EQ(GenericValue::CreateBoolean(false), result->GetValue(0));

  // result[1] = NULL && true = NULL
  EXPECT_EQ(GenericValue::CreateNull(TypeId::Boolean), result->GetValue(1));

  // result[2] = true && true = true
  EXPECT_EQ(GenericValue::CreateBoolean(true), result->GetValue(2));
}

TEST_F(VectorOperationsTest, Select) {
  // a = [NULL, 1, 2, 3, 4, 5]
  // b = [NULL, 1, 4, 3, 5, 5]
  auto a = MakeTinyIntVector({0, 1, 2, 3, 4, 5},
                             {true, false, false, false, false, false});
  auto b = MakeTinyIntVector({0, 1, 4, 3, 5, 5},
                             {true, false, false, false, false, false});
  auto _2 = ConstantVector(GenericValue::CreateTinyInt(2));
  auto result = std::array<sel_t, kDefaultVectorSize>();

  u32 n;
  for (auto type_id : {TypeId::TinyInt, TypeId::SmallInt, TypeId::Integer,
                       TypeId::BigInt, TypeId::Float, TypeId::Double}) {
    a->Cast(type_id);
    b->Cast(type_id);
    _2.Cast(type_id);

    // a < 2
    VectorOps::SelectLessThan(*a, _2, result.data(), &n);
    EXPECT_EQ(1u, n);
    EXPECT_EQ(1u, result[0]);

    // 2 < a
    VectorOps::SelectLessThan(_2, *a, result.data(), &n);
    EXPECT_EQ(3u, n);
    EXPECT_EQ(3u, result[0]);
    EXPECT_EQ(4u, result[1]);
    EXPECT_EQ(5u, result[2]);

    // 2 == a
    VectorOps::SelectEqual(_2, *a, result.data(), &n);
    EXPECT_EQ(1u, n);
    EXPECT_EQ(2u, result[0]);

    // a != b = [
    VectorOps::SelectNotEqual(*a, *b, result.data(), &n);
    EXPECT_EQ(2u, n);
    EXPECT_EQ(2u, result[0]);
    EXPECT_EQ(4u, result[1]);

    // b == a
    VectorOps::SelectEqual(*b, *a, result.data(), &n);
    EXPECT_EQ(3u, n);
    EXPECT_EQ(1u, result[0]);
    EXPECT_EQ(3u, result[1]);
    EXPECT_EQ(5u, result[2]);
  }
}

}  // namespace tpl::sql::test
