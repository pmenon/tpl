#include "sql_test.h"

#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

class VectorFillTest : public TplTest {};

TEST_F(VectorFillTest, SimpleNonNull) {
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

TEST_F(VectorFillTest, Null) {
  // Fill a vector with the given type with the given value of that type
#define CHECK_SIMPLE_FILL(TYPE)  \
  {                                          \
    auto vec = Make##TYPE##Vector(10);       \
    VectorOps::FillNull(vec.get());          \
    for (u64 i = 0; i < vec->count(); i++) { \
      auto val = vec->GetValue(i);           \
      EXPECT_TRUE(val.is_null());           \
    }                                        \
  }

  CHECK_SIMPLE_FILL(Boolean);
  CHECK_SIMPLE_FILL(TinyInt);
  CHECK_SIMPLE_FILL(SmallInt);
  CHECK_SIMPLE_FILL(Integer);
  CHECK_SIMPLE_FILL(BigInt);
  CHECK_SIMPLE_FILL(Float);
  CHECK_SIMPLE_FILL(Double);
  CHECK_SIMPLE_FILL(Varchar);
#undef CHECK_SIMPLE_FILL
}

}  // namespace tpl::sql
