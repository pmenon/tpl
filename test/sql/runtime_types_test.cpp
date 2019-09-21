#include "sql/runtime_types.h"
#include "common/exception.h"
#include "util/test_harness.h"

namespace tpl::sql {

class RuntimeTypesTest : public TplTest {};

TEST_F(RuntimeTypesTest, ExtractDateParts) {
  // Valid date
  Date d;
  EXPECT_NO_THROW({ d = Date::FromYMD(2016, 12, 19); });
  EXPECT_EQ(2016u, d.ExtractYear());
  EXPECT_EQ(12u, d.ExtractMonth());
  EXPECT_EQ(19u, d.ExtractDay());

  // Invalid
  EXPECT_THROW({ d = Date::FromYMD(1234, 3, 1111); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(1234, 93874, 11); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(1234, 7283, 192873); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(88888, 12, 12); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(88888, 12, 987); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(88888, 921873, 1); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(88888, 921873, 21938); }, ConversionException);
}

TEST_F(RuntimeTypesTest, DateFromString) {
  // Valid date
  Date d;
  EXPECT_NO_THROW({ d = Date::FromString("1990-01-11"); });
  EXPECT_EQ(1990u, d.ExtractYear());
  EXPECT_EQ(1u, d.ExtractMonth());
  EXPECT_EQ(11u, d.ExtractDay());

  // Invalid
  EXPECT_THROW({ d = Date::FromString("1000-11-23123"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("1000-12323-19"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("1000-12323-199"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("129398-12-20"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("129398-12-120"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("129398-1289217-12"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("da fuk?"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("-1-1-,23"); }, ConversionException);
}

TEST_F(RuntimeTypesTest, DateComparisons) {
  Date d1 = Date::FromString("2000-01-01");
  Date d2 = Date::FromString("2016-02-19");
  Date d3 = d1;
  Date d4 = Date::FromString("2017-10-10");
  EXPECT_NE(d1, d2);
  EXPECT_LT(d1, d2);
  EXPECT_EQ(d1, d3);
  EXPECT_GT(d4, d3);
  EXPECT_GT(d4, d2);
  EXPECT_GT(d4, d1);
}

TEST_F(RuntimeTypesTest, DateToString) {
  Date d1 = Date::FromString("2016-01-27");
  EXPECT_EQ("2016-01-27", d1.ToString());

  // Make sure we pad months and days
  d1 = Date::FromString("2000-01-01");
  EXPECT_EQ("2000-01-01", d1.ToString());
}

}  // namespace tpl::sql