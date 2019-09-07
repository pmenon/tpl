#include <vector>

#include "sql_test.h"

#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

class VectorGenerateTest : public TplTest {};

TEST_F(VectorGenerateTest, Simple) {
  const uint32_t num_elems = 50;

// Generate odd sequence of numbers starting at 1 inclusive. In other words,
// generate the values [2*i+1 for i in range(0,50)]
#define CHECK_SIMPLE_GENERATE(TYPE)                          \
  {                                                          \
    auto vec = Make##TYPE##Vector(num_elems);                \
    VectorOps::Generate(vec.get(), 1, 2);                    \
    for (uint64_t i = 0; i < vec->count(); i++) {            \
      auto val = vec->GetValue(i);                           \
      EXPECT_FALSE(val.is_null());                           \
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

}  // namespace tpl::sql
