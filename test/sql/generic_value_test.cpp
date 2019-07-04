#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "sql/generic_value.h"

namespace tpl::sql::test {

class GenericValueTests : public TplTest {};

TEST_F(GenericValueTests, Boolean) {
  {
    auto value = GenericValue::CreateBoolean(true);
    EXPECT_FALSE(value.is_null());
    EXPECT_EQ(TypeId::Boolean, value.type_id());
  }

  {
    auto value = GenericValue::CreateBoolean(false);
    EXPECT_FALSE(value.is_null());
    EXPECT_EQ(TypeId::Boolean, value.type_id());
  }
}

TEST_F(GenericValueTests, TinyInt) {
  auto value = GenericValue::CreateTinyInt(1);
  EXPECT_FALSE(value.is_null());
  EXPECT_EQ(TypeId::TinyInt, value.type_id());
}

TEST_F(GenericValueTests, SmallInt) {
  auto value = GenericValue::CreateSmallInt(10);
  EXPECT_FALSE(value.is_null());
  EXPECT_EQ(TypeId::SmallInt, value.type_id());
}

TEST_F(GenericValueTests, Int) {
  auto value = GenericValue::CreateInteger(100);
  EXPECT_FALSE(value.is_null());
  EXPECT_EQ(TypeId::Integer, value.type_id());
}

TEST_F(GenericValueTests, BigInt) {
  auto value = GenericValue::CreateBigInt(1000);
  EXPECT_FALSE(value.is_null());
  EXPECT_EQ(TypeId::BigInt, value.type_id());
}

TEST_F(GenericValueTests, Hash) {
  auto value = GenericValue::CreateHash(hash_t{10000});
  EXPECT_FALSE(value.is_null());
  EXPECT_EQ(TypeId::Hash, value.type_id());
}

TEST_F(GenericValueTests, Pointer) {
  {
    i32 x = 10;
    auto value = GenericValue::CreatePointer(&x);
    EXPECT_FALSE(value.is_null());
    EXPECT_EQ(TypeId::Pointer, value.type_id());
  }
}

}  // namespace tpl::sql::test