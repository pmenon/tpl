#include <numeric>

#include "tpl_test.h"  // NOLINT

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

TEST_F(VectorOperationsTest, CompareEqual) {
  const u32 num_elems = 100;
  Vector vec(TypeId::BigInt, true, false);
  vec.set_count(num_elems);
  VectorOps::Generate(&vec, 0, 1);
}

}  // namespace tpl::sql::test
