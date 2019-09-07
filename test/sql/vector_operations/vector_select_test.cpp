#include <vector>

#include "sql_test.h"  // NOLINT

#include "sql/constant_vector.h"
#include "sql/tuple_id_list.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

class VectorSelectTest : public TplTest {};

TEST_F(VectorSelectTest, Select) {
  // a = [NULL, 1, 2, 3, 4, 5]
  // b = [NULL, 1, 4, 3, 5, 5]
  auto a = MakeTinyIntVector({0, 1, 2, 3, 4, 5}, {true, false, false, false, false, false});
  auto b = MakeTinyIntVector({0, 1, 4, 3, 5, 5}, {true, false, false, false, false, false});
  auto _2 = ConstantVector(GenericValue::CreateTinyInt(2));
  auto result = std::array<sel_t, kDefaultVectorSize>();

  uint32_t n;
  for (auto type_id : {TypeId::TinyInt, TypeId::SmallInt, TypeId::Integer, TypeId::BigInt,
                       TypeId::Float, TypeId::Double}) {
    a->Cast(type_id);
    b->Cast(type_id);
    _2.Cast(type_id);

    // a < 2
    n = VectorOps::SelectLessThan(*a, _2, result.data());
    EXPECT_EQ(1u, n);
    EXPECT_EQ(1u, result[0]);

    // 2 < a
    n = VectorOps::SelectLessThan(_2, *a, result.data());
    EXPECT_EQ(3u, n);
    EXPECT_EQ(3u, result[0]);
    EXPECT_EQ(4u, result[1]);
    EXPECT_EQ(5u, result[2]);

    // 2 == a
    n = VectorOps::SelectEqual(_2, *a, result.data());
    EXPECT_EQ(1u, n);
    EXPECT_EQ(2u, result[0]);

    // a != b
    n = VectorOps::SelectNotEqual(*a, *b, result.data());
    EXPECT_EQ(2u, n);
    EXPECT_EQ(2u, result[0]);
    EXPECT_EQ(4u, result[1]);

    // b == a
    n = VectorOps::SelectEqual(*b, *a, result.data());
    EXPECT_EQ(3u, n);
    EXPECT_EQ(1u, result[0]);
    EXPECT_EQ(3u, result[1]);
    EXPECT_EQ(5u, result[2]);
  }
}

}  // namespace tpl::sql
