#include "tpl_test.h"  // NOLINT

#include "sql/data_types.h"

namespace tpl::sql::test {

class DataTypesTests : public TplTest {};

#define CHECK_TYPE_PROPERTIES(TYPE, TYPE_ID, IS_ARITHMETIC) \
  const auto &type = TYPE::Instance(true);                  \
  EXPECT_TRUE(type.nullable());                             \
  EXPECT_EQ(TYPE_ID, type.type_id());                       \
  EXPECT_EQ(IS_ARITHMETIC, type.IsArithmetic());            \
  EXPECT_TRUE(type.Equals(TYPE::InstanceNullable()));       \
  EXPECT_FALSE(type.Equals(TYPE::InstanceNonNullable()));

#define CHECK_NOT_EQUAL(INSTANCE, OTHER_TYPE)                \
  EXPECT_FALSE(INSTANCE.Equals(OTHER_TYPE::Instance(true))); \
  EXPECT_FALSE(INSTANCE.Equals(OTHER_TYPE::Instance(false)));

#define CHECK_NOT_EQUAL_ALT(INSTANCE, OTHER_TYPE, ...)                    \
  EXPECT_FALSE(INSTANCE.Equals(OTHER_TYPE::Instance(true, __VA_ARGS__))); \
  EXPECT_FALSE(INSTANCE.Equals(OTHER_TYPE::Instance(false, __VA_ARGS__)));

TEST_F(DataTypesTests, BooleanType) {
  CHECK_TYPE_PROPERTIES(BooleanType, TypeId::Boolean, false);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DateType);
}

TEST_F(DataTypesTests, SmallIntType) {
  CHECK_TYPE_PROPERTIES(SmallIntType, TypeId::SmallInt, true);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DateType);
}

TEST_F(DataTypesTests, IntegerType) {
  CHECK_TYPE_PROPERTIES(IntegerType, TypeId::Integer, true);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DateType);
}

TEST_F(DataTypesTests, BigIntType) {
  CHECK_TYPE_PROPERTIES(BigIntType, TypeId::BigInt, true);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DateType);
}

TEST_F(DataTypesTests, RealType) {
  CHECK_TYPE_PROPERTIES(RealType, TypeId::Real, true);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, DateType);
}

TEST_F(DataTypesTests, DateType) {
  CHECK_TYPE_PROPERTIES(DateType, TypeId::Date, false);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, RealType);
}

TEST_F(DataTypesTests, DecimalType) {
  const auto &type1 = DecimalType::InstanceNullable(5, 2);
  EXPECT_TRUE(type1.nullable());
  EXPECT_EQ(TypeId::Decimal, type1.type_id());
  EXPECT_EQ(5u, type1.precision());
  EXPECT_EQ(2u, type1.scale());
  EXPECT_TRUE(type1.IsArithmetic());

  CHECK_NOT_EQUAL(type1, BooleanType);
  CHECK_NOT_EQUAL(type1, SmallIntType);
  CHECK_NOT_EQUAL(type1, IntegerType);
  CHECK_NOT_EQUAL(type1, BigIntType);
  CHECK_NOT_EQUAL(type1, RealType);
  CHECK_NOT_EQUAL_ALT(type1, DecimalType, 3, 4);

  EXPECT_FALSE(type1.Equals(DecimalType::InstanceNonNullable(5, 2)));
  EXPECT_TRUE(type1.Equals(DecimalType::InstanceNullable(5, 2)));
}

TEST_F(DataTypesTests, CharType) {
  const auto &type1 = CharType::InstanceNullable(100);
  EXPECT_TRUE(type1.nullable());
  EXPECT_EQ(TypeId::Char, type1.type_id());
  EXPECT_EQ(100u, type1.length());
  EXPECT_FALSE(type1.IsArithmetic());

  CHECK_NOT_EQUAL(type1, BooleanType);
  CHECK_NOT_EQUAL(type1, SmallIntType);
  CHECK_NOT_EQUAL(type1, IntegerType);
  CHECK_NOT_EQUAL(type1, BigIntType);
  CHECK_NOT_EQUAL(type1, RealType);
  CHECK_NOT_EQUAL(type1, DateType);
  CHECK_NOT_EQUAL_ALT(type1, DecimalType, 5, 2);
  CHECK_NOT_EQUAL_ALT(type1, CharType, 200);
  CHECK_NOT_EQUAL_ALT(type1, VarcharType, 200);

  EXPECT_TRUE(type1.Equals(CharType::InstanceNullable(100)));
}

TEST_F(DataTypesTests, VarcharType) {
  const auto &type1 = VarcharType::InstanceNullable(100);
  EXPECT_TRUE(type1.nullable());
  EXPECT_EQ(TypeId::Varchar, type1.type_id());
  EXPECT_EQ(100u, type1.max_length());
  EXPECT_FALSE(type1.IsArithmetic());

  CHECK_NOT_EQUAL(type1, BooleanType);
  CHECK_NOT_EQUAL(type1, SmallIntType);
  CHECK_NOT_EQUAL(type1, IntegerType);
  CHECK_NOT_EQUAL(type1, BigIntType);
  CHECK_NOT_EQUAL(type1, RealType);
  CHECK_NOT_EQUAL(type1, DateType);
  CHECK_NOT_EQUAL_ALT(type1, DecimalType, 5, 2);
  CHECK_NOT_EQUAL_ALT(type1, VarcharType, 200);

  EXPECT_TRUE(type1.Equals(VarcharType::InstanceNullable(100)));
}

}  // namespace tpl::sql::test