#include <vector>

#include "sql/constant_vector.h"
#include "sql/tuple_id_list.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/sql_test_harness.h"

namespace tpl::sql {

class VectorSelectTest : public TplTest {};

TEST_F(VectorSelectTest, Select) {
  // a = [NULL, 1, 2, 3, 4, 5]
  // b = [NULL, 1, 4, 3, 5, 5]
  auto a = MakeTinyIntVector({0, 1, 2, 3, 4, 5},
                             {true, false, false, false, false, false});
  auto b = MakeTinyIntVector({0, 1, 4, 3, 5, 5},
                             {true, false, false, false, false, false});
  auto _2 = ConstantVector(GenericValue::CreateTinyInt(2));

  for (auto type_id : {TypeId::TinyInt, TypeId::SmallInt, TypeId::Integer,
                       TypeId::BigInt, TypeId::Float, TypeId::Double}) {
    a->Cast(type_id);
    b->Cast(type_id);
    _2.Cast(type_id);

    TupleIdList input_list(a->num_elements());
    input_list.AddAll();

    // a < 2
    VectorOps::SelectLessThan(*a, _2, &input_list);
    EXPECT_EQ(1u, input_list.GetTupleCount());
    EXPECT_TRUE(input_list.Contains(1));

    input_list.AddAll();

    // 2 < a
    VectorOps::SelectLessThan(_2, *a, &input_list);
    EXPECT_EQ(3u, input_list.GetTupleCount());
    EXPECT_EQ(3u, input_list[0]);
    EXPECT_EQ(4u, input_list[1]);
    EXPECT_EQ(5u, input_list[2]);

    input_list.AddAll();

    // 2 == a
    VectorOps::SelectEqual(_2, *a, &input_list);
    EXPECT_EQ(1u, input_list.GetTupleCount());
    EXPECT_EQ(2u, input_list[0]);

    input_list.AddAll();

    // a != b
    VectorOps::SelectNotEqual(*a, *b, &input_list);
    EXPECT_EQ(2u, input_list.GetTupleCount());
    EXPECT_EQ(2u, input_list[0]);
    EXPECT_EQ(4u, input_list[1]);

    input_list.AddAll();

    // b == a
    VectorOps::SelectEqual(*b, *a, &input_list);
    EXPECT_EQ(3u, input_list.GetTupleCount());
    EXPECT_EQ(1u, input_list[0]);
    EXPECT_EQ(3u, input_list[1]);
    EXPECT_EQ(5u, input_list[2]);
  }
}

}  // namespace tpl::sql
