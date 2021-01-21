#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "sql/functions/comparison_functions.h"
#include "sql/value.h"
#include "util/test_harness.h"
#include "util/timer.h"

namespace tpl::sql {

class ComparisonFunctionsTests : public TplTest {};

TEST_F(ComparisonFunctionsTests, NullComparison) {
#define CHECK_NULL(TYPE, OP, INITIAL)       \
  {                                         \
    TYPE a = TYPE::Null(), b(INITIAL);      \
    BoolVal result(false);                  \
    ComparisonFunctions::OP(&result, a, b); \
    EXPECT_TRUE(result.is_null);            \
  }
#define CHECK_NULL_FOR_ALL_COMPARISONS(TYPE, INITIAL) \
  CHECK_NULL(TYPE, Equal, INITIAL)                    \
  CHECK_NULL(TYPE, GreaterEqual, INITIAL)             \
  CHECK_NULL(TYPE, GreaterThan, INITIAL)              \
  CHECK_NULL(TYPE, LessEqual, INITIAL)                \
  CHECK_NULL(TYPE, LessThan, INITIAL)                 \
  CHECK_NULL(TYPE, NotEqual, INITIAL)

  CHECK_NULL_FOR_ALL_COMPARISONS(BoolVal, true);
  CHECK_NULL_FOR_ALL_COMPARISONS(Integer, 0);
  CHECK_NULL_FOR_ALL_COMPARISONS(Real, 0.0);
  CHECK_NULL_FOR_ALL_COMPARISONS(StringVal, "");

#undef CHECK_NULL_FOR_ALL_COMPARISONS
#undef CHECK_NULL
}

#define CHECK_OP(TYPE, OP, INPUT1, INPUT2, EXPECTED) \
  {                                                  \
    TYPE a(INPUT1), b(INPUT2);                       \
    BoolVal result(false);                           \
    ComparisonFunctions::OP(&result, a, b);          \
    EXPECT_FALSE(result.is_null);                    \
    EXPECT_EQ(EXPECTED, result.val);                 \
  }
#define CHECK_ALL_COMPARISONS(TYPE, INPUT1, INPUT2)                \
  CHECK_OP(TYPE, Equal, INPUT1, INPUT2, (INPUT1 == INPUT2))        \
  CHECK_OP(TYPE, GreaterEqual, INPUT1, INPUT2, (INPUT1 >= INPUT2)) \
  CHECK_OP(TYPE, GreaterThan, INPUT1, INPUT2, (INPUT1 > INPUT2))   \
  CHECK_OP(TYPE, LessEqual, INPUT1, INPUT2, (INPUT1 <= INPUT2))    \
  CHECK_OP(TYPE, LessThan, INPUT1, INPUT2, (INPUT1 < INPUT2))      \
  CHECK_OP(TYPE, NotEqual, INPUT1, INPUT2, (INPUT1 != INPUT2))

TEST_F(ComparisonFunctionsTests, IntegerComparison) {
  CHECK_ALL_COMPARISONS(Integer, 10, 20);
  CHECK_ALL_COMPARISONS(Integer, -10, 20);
  CHECK_ALL_COMPARISONS(Integer, 0, 0);
  CHECK_ALL_COMPARISONS(Integer, -213, -376);
}

TEST_F(ComparisonFunctionsTests, RealComparison) {
  CHECK_ALL_COMPARISONS(Real, 0.0, 0.0);
  CHECK_ALL_COMPARISONS(Real, 1.0, 0.0);
  CHECK_ALL_COMPARISONS(Real, -1.0, 0.0);
  CHECK_ALL_COMPARISONS(Real, 1.0, -2.0);
}

TEST_F(ComparisonFunctionsTests, BoolValComparison) {
  CHECK_ALL_COMPARISONS(BoolVal, false, false);
  CHECK_ALL_COMPARISONS(BoolVal, true, false);
  CHECK_ALL_COMPARISONS(BoolVal, false, true);
  CHECK_ALL_COMPARISONS(BoolVal, true, true);
}

#undef CHECK_ALL_COMPARISONS
#undef CHECK_NULL

TEST_F(ComparisonFunctionsTests, StringComparison) {
#define CHECK(INPUT1, INPUT2, OP, EXPECTED) \
  {                                         \
    BoolVal result = BoolVal::Null();       \
    StringVal x(INPUT1), y(INPUT2);         \
    ComparisonFunctions::OP(&result, x, y); \
    EXPECT_FALSE(result.is_null);           \
    EXPECT_EQ(EXPECTED, result.val);        \
  }

  // Same sizes
  CHECK("test", "test", Equal, true);
  CHECK("test", "test", GreaterEqual, true);
  CHECK("test", "test", GreaterThan, false);
  CHECK("test", "test", LessEqual, true);
  CHECK("test", "test", LessThan, false);
  CHECK("test", "test", NotEqual, false);

  // Different sizes
  CHECK("test", "testholla", Equal, false);
  CHECK("test", "testholla", GreaterEqual, false);
  CHECK("test", "testholla", GreaterThan, false);
  CHECK("test", "testholla", LessEqual, true);
  CHECK("test", "testholla", LessThan, true);
  CHECK("test", "testholla", NotEqual, true);

  // Different sizes
  CHECK("testholla", "test", Equal, false);
  CHECK("testholla", "test", GreaterEqual, true);
  CHECK("testholla", "test", GreaterThan, true);
  CHECK("testholla", "test", LessEqual, false);
  CHECK("testholla", "test", LessThan, false);
  CHECK("testholla", "test", NotEqual, true);

  CHECK("testholla", "", Equal, false);
  CHECK("testholla", "", GreaterEqual, true);
  CHECK("testholla", "", GreaterThan, true);
  CHECK("testholla", "", LessEqual, false);
  CHECK("testholla", "", LessThan, false);
  CHECK("testholla", "", NotEqual, true);

  CHECK("", "", Equal, true);
  CHECK("", "", GreaterEqual, true);
  CHECK("", "", GreaterThan, false);
  CHECK("", "", LessEqual, true);
  CHECK("", "", LessThan, false);
  CHECK("", "", NotEqual, false);

#undef CHECK
}

}  // namespace tpl::sql
