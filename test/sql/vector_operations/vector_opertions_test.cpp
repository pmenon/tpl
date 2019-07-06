#include <numeric>

#include "tpl_test.h"  // NOLINT

#include "sql/sql_type_traits.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql::test {

class VectorOperationsTest : public TplTest {};

TEST_F(VectorOperationsTest, Fill) {
#define CHECK_SIMPLE_FILL(TYPE_ID, FILL_VALUE)                             \
  {                                                                        \
    Vector vec(TYPE_ID, true, false);                                      \
    vec.set_count(10);                                                     \
    VectorOps::Fill(&vec, FILL_VALUE);                                     \
    for (u32 i = 0; i < vec.count(); i++) {                                \
      auto *ptr = vec.GetValue<TypeTraits<TYPE_ID>::CppType>(i);           \
      if constexpr (std::is_same<f32, TypeTraits<TYPE_ID>::CppType>()) {   \
        EXPECT_FLOAT_EQ(FILL_VALUE, *ptr);                                 \
      } else if constexpr (std::is_same<f64,                               \
                                        TypeTraits<TYPE_ID>::CppType>()) { \
        EXPECT_DOUBLE_EQ(FILL_VALUE, *ptr);                                \
      } else {                                                             \
        EXPECT_EQ(FILL_VALUE, *ptr);                                       \
      }                                                                    \
    }                                                                      \
  }

  CHECK_SIMPLE_FILL(TypeId::Boolean, true);
  CHECK_SIMPLE_FILL(TypeId::TinyInt, i64(-24));
  CHECK_SIMPLE_FILL(TypeId::SmallInt, i64(47));
  CHECK_SIMPLE_FILL(TypeId::Integer, i64(1234));
  CHECK_SIMPLE_FILL(TypeId::BigInt, i64(-24987));
  CHECK_SIMPLE_FILL(TypeId::Float, f64(-3.10));
  CHECK_SIMPLE_FILL(TypeId::Double, f64(-3.14));
}

}  // namespace tpl::sql::test
