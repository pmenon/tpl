#include "sql/functions/is_null_predicate.h"
#include "sql/value.h"
#include "util/test_harness.h"

namespace tpl::sql {

class IsNullPredicateTests : public TplTest {};

TEST_F(IsNullPredicateTests, IsNull) {
#define CHECK_NULL_FOR_TYPE(NullVal, NonNullVal)                   \
  {                                                                \
    EXPECT_TRUE(tpl::sql::IsNullPredicate::IsNull(NullVal));       \
    EXPECT_FALSE(tpl::sql::IsNullPredicate::IsNotNull(NullVal));   \
    EXPECT_FALSE(tpl::sql::IsNullPredicate::IsNull(NonNullVal));   \
    EXPECT_TRUE(tpl::sql::IsNullPredicate::IsNotNull(NonNullVal)); \
  }

  CHECK_NULL_FOR_TYPE(BoolVal::Null(), BoolVal(false));
  CHECK_NULL_FOR_TYPE(Integer::Null(), Integer(44));
  CHECK_NULL_FOR_TYPE(Real::Null(), Real(44.0));
  CHECK_NULL_FOR_TYPE(StringVal::Null(), StringVal("44"));
  CHECK_NULL_FOR_TYPE(DateVal::Null(), DateVal(sql::Date::FromYMD(2010, 10, 10)));
  // CHECK_IS_NOT_NULL_FOR_TYPE(TimestampVal::Null(), sql::Timestamp::FromString("2010-10-10"));
}

}  // namespace tpl::sql
