#include "sql/constant_vector.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/sql_test_harness.h"
#include "util/test_harness.h"

namespace tpl::sql {

class VectorBitwiseTest : public TplTest {};

TEST_F(VectorBitwiseTest, InPlaceBitwiseAND) {
#define GEN_CASE(TYPE, CPP_TYPE)                                                          \
  {                                                                                       \
    auto a = Make##TYPE##Vector(100);                                                     \
    VectorOps::Generate(a.get(), 0, 2);                                                   \
    VectorOps::BitwiseAndInPlace(a.get(), ConstantVector(GenericValue::Create##TYPE(3))); \
    EXPECT_EQ(100, a->GetSize());                                                         \
    EXPECT_EQ(100, a->GetCount());                                                        \
    EXPECT_EQ(nullptr, a->GetFilteredTupleIdList());                                      \
    auto *a_data = reinterpret_cast<CPP_TYPE *>(a->GetData());                            \
    for (uint64_t i = 0; i < a->GetCount(); i++) {                                        \
      EXPECT_FALSE(a->IsNull(i));                                                         \
      EXPECT_LE(a_data[i], 3);                                                            \
    }                                                                                     \
  }

  GEN_CASE(TinyInt, int8_t);
  GEN_CASE(SmallInt, int16_t);
  GEN_CASE(Integer, int32_t);
  GEN_CASE(BigInt, int64_t);
  GEN_CASE(Pointer, uintptr_t);
#undef GEN_CASE
}

}  // namespace tpl::sql
