#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "sql/generic_value.h"

namespace tpl::sql {

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
  i32 x = 10;
  auto value = GenericValue::CreatePointer(&x);
  EXPECT_FALSE(value.is_null());
  EXPECT_EQ(TypeId::Pointer, value.type_id());
}

TEST_F(GenericValueTests, Equality) {
  auto bigint_val = GenericValue::CreateBigInt(19);
  EXPECT_EQ(bigint_val, GenericValue::CreateBigInt(19));
  EXPECT_NE(bigint_val, GenericValue::CreateInteger(1));
  EXPECT_NE(bigint_val, GenericValue::CreateReal(1));
  EXPECT_NE(bigint_val, GenericValue::CreateVarchar("blah"));
  EXPECT_NE(bigint_val, GenericValue::CreateNull(TypeId::BigInt));

  auto real_val = GenericValue::CreateReal(10.34f);
  EXPECT_EQ(real_val, GenericValue::CreateReal(10.34f));
  EXPECT_NE(real_val, GenericValue::CreateInteger(1));
  EXPECT_NE(real_val, GenericValue::CreateReal(1));
  EXPECT_NE(real_val, GenericValue::CreateDouble(1));
  EXPECT_NE(real_val, GenericValue::CreateVarchar("blah"));
  EXPECT_NE(real_val, GenericValue::CreateNull(TypeId::Float));

  auto string_val = GenericValue::CreateVarchar("hello");
  EXPECT_EQ(string_val, GenericValue::CreateVarchar("hello"));
  EXPECT_NE(string_val, GenericValue::CreateInteger(1));
  EXPECT_NE(string_val, GenericValue::CreateReal(1));
  EXPECT_NE(string_val, GenericValue::CreateDouble(1));
  EXPECT_NE(string_val, GenericValue::CreateVarchar("blah"));
  EXPECT_NE(string_val, GenericValue::CreateNull(TypeId::Varchar));
}

}  // namespace tpl::sql
