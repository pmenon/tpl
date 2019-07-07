#include <numeric>

#include "tpl_test.h"  // NOLINT

#include "sql/constant_vector.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql::test {

class VectorOperationsTest : public TplTest {};

TEST_F(VectorOperationsTest, Generate) {
  const u32 num_elems = 50;

  // Generate odd sequence of numbers starting at 1 inclusive. In other words,
  // generate the values [2*i+1 for i in range(0,50)]
#define CHECK_SIMPLE_GENERATE(TYPE)                               \
  {                                                               \
    Vector vec(TypeId::TYPE, true, false);                        \
    vec.set_count(num_elems);                                     \
    VectorOps::Generate(&vec, 1, 2);                              \
    for (u32 i = 0; i < vec.count(); i++) {                       \
      auto val = vec.GetValue(i);                                 \
      EXPECT_EQ(GenericValue::Create##TYPE(2 * i32(i) + 1), val); \
    }                                                             \
  }

  CHECK_SIMPLE_GENERATE(TinyInt)
  CHECK_SIMPLE_GENERATE(SmallInt)
  CHECK_SIMPLE_GENERATE(Integer)
  CHECK_SIMPLE_GENERATE(BigInt)
  CHECK_SIMPLE_GENERATE(Float)
  CHECK_SIMPLE_GENERATE(Double)
}

TEST_F(VectorOperationsTest, Fill) {
#define CHECK_SIMPLE_FILL(TYPE, FILL_VALUE)                        \
  {                                                                \
    Vector vec(TypeId::TYPE, true, false);                         \
    vec.set_count(10);                                             \
    VectorOps::Fill(&vec, GenericValue::Create##TYPE(FILL_VALUE)); \
    for (u32 i = 0; i < vec.count(); i++) {                        \
      auto val = vec.GetValue(i);                                  \
      EXPECT_EQ(GenericValue::Create##TYPE(FILL_VALUE), val);      \
    }                                                              \
  }

  CHECK_SIMPLE_FILL(Boolean, true);
  CHECK_SIMPLE_FILL(TinyInt, i64(-24));
  CHECK_SIMPLE_FILL(SmallInt, i64(47));
  CHECK_SIMPLE_FILL(Integer, i64(1234));
  CHECK_SIMPLE_FILL(BigInt, i64(-24987));
  CHECK_SIMPLE_FILL(Float, f64(-3.10));
  CHECK_SIMPLE_FILL(Double, f64(-3.14));
}

TEST_F(VectorOperationsTest, CompareWithConstant) {
  // Input vector: [0,1,2,3,4,5]
  Vector vec(TypeId::BigInt, true, false);
  vec.set_count(6);
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

TEST_F(VectorOperationsTest, CompareWithNulls) {
  const u32 num_elems = 6;
  Vector vec(TypeId::BigInt, true, false);
  vec.set_count(num_elems);
  VectorOps::Generate(&vec, 0, 1);

  ConstantVector _null(GenericValue::CreateNull(TypeId::BigInt));
  Vector result(TypeId::Boolean, true, true);

  VectorOps::Equal(vec, _null, &result);
  EXPECT_TRUE(result.GetValue(0).is_null());
  EXPECT_TRUE(result.GetValue(1).is_null());
  EXPECT_TRUE(result.GetValue(2).is_null());
  EXPECT_TRUE(result.GetValue(3).is_null());
  EXPECT_TRUE(result.GetValue(4).is_null());
  EXPECT_TRUE(result.GetValue(5).is_null());
}

TEST_F(VectorOperationsTest, NullChecking) {
  Vector vec(TypeId::Float, true, false);
  vec.set_count(4);

  vec.SetValue(0, GenericValue::CreateFloat(1.0));
  vec.SetValue(1, GenericValue::CreateNull(TypeId::Float));
  vec.SetValue(2, GenericValue::CreateFloat(1.0));
  vec.SetValue(3, GenericValue::CreateNull(TypeId::Float));

  {
    Vector result(TypeId::Boolean, true, true);
    VectorOps::IsNull(vec, &result);
    EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(0));
    EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(1));
    EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(2));
    EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(3));
  }

  {
    Vector result(TypeId::Boolean, true, true);
    VectorOps::IsNotNull(vec, &result);
    EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(0));
    EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(1));
    EXPECT_EQ(GenericValue::CreateBoolean(true), result.GetValue(2));
    EXPECT_EQ(GenericValue::CreateBoolean(false), result.GetValue(3));
  }
}

TEST_F(VectorOperationsTest, AnyOrAllTrue) {
  Vector vec(TypeId::Boolean, true, false);
  vec.set_count(4);

  vec.SetValue(0, GenericValue::CreateBoolean(false));
  vec.SetValue(1, GenericValue::CreateBoolean(false));
  vec.SetValue(2, GenericValue::CreateBoolean(false));
  vec.SetValue(3, GenericValue::CreateBoolean(false));

  EXPECT_FALSE(VectorOps::AnyTrue(vec));
  EXPECT_FALSE(VectorOps::AllTrue(vec));

  vec.SetValue(3, GenericValue::CreateNull(TypeId::Boolean));

  EXPECT_FALSE(VectorOps::AnyTrue(vec));
  EXPECT_FALSE(VectorOps::AllTrue(vec));

  vec.SetValue(3, GenericValue::CreateBoolean(true));

  EXPECT_TRUE(VectorOps::AnyTrue(vec));
  EXPECT_FALSE(VectorOps::AllTrue(vec));

  vec.SetValue(0, GenericValue::CreateBoolean(true));
  vec.SetValue(1, GenericValue::CreateBoolean(true));
  vec.SetValue(2, GenericValue::CreateBoolean(true));
  vec.SetValue(3, GenericValue::CreateBoolean(true));

  EXPECT_TRUE(VectorOps::AnyTrue(vec));
  EXPECT_TRUE(VectorOps::AllTrue(vec));
}

}  // namespace tpl::sql::test
