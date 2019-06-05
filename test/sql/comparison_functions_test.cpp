#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "sql/functions/comparison_functions.h"
#include "sql/value.h"
#include "util/timer.h"

namespace tpl::sql::test {

class ComparisonFunctionsTests : public TplTest {};

TEST_F(ComparisonFunctionsTests, NullComparisonTests) {
// Nulls
#define CHECK_NULL(TYPE, OP)                      \
  {                                               \
    TYPE a = TYPE::Null(), b(0);                  \
    BoolVal result(false);                        \
    ComparisonFunctions::OP##TYPE(&result, a, b); \
    EXPECT_TRUE(result.is_null);                  \
  }
#define CHECK_NULL_FOR_ALL_COMPARISONS(TYPE) \
  CHECK_NULL(Integer, Eq)                    \
  CHECK_NULL(Integer, Ge)                    \
  CHECK_NULL(Integer, Gt)                    \
  CHECK_NULL(Integer, Le)                    \
  CHECK_NULL(Integer, Lt)                    \
  CHECK_NULL(Integer, Ne)

  CHECK_NULL_FOR_ALL_COMPARISONS(BoolVal);
  CHECK_NULL_FOR_ALL_COMPARISONS(Integer);
  CHECK_NULL_FOR_ALL_COMPARISONS(Real);

#undef CHECK_NULL_FOR_ALL_COMPARISONS
#undef CHECK_NULL
}

TEST_F(ComparisonFunctionsTests, SimpleComparisonTests) {
#define CHECK_OP(TYPE, OP, INPUT1, INPUT2, EXPECTED) \
  {                                                  \
    TYPE a(INPUT1), b(INPUT2);                       \
    BoolVal result(false);                           \
    ComparisonFunctions::OP##TYPE(&result, a, b);    \
    EXPECT_FALSE(result.is_null);                    \
    EXPECT_EQ(EXPECTED, result.val);                 \
  }
#define CHECK_ALL_COMPARISONS(TYPE, INPUT1, INPUT2)      \
  CHECK_OP(TYPE, Eq, INPUT1, INPUT2, (INPUT1 == INPUT2)) \
  CHECK_OP(TYPE, Ge, INPUT1, INPUT2, (INPUT1 >= INPUT2)) \
  CHECK_OP(TYPE, Gt, INPUT1, INPUT2, (INPUT1 > INPUT2))  \
  CHECK_OP(TYPE, Le, INPUT1, INPUT2, (INPUT1 <= INPUT2)) \
  CHECK_OP(TYPE, Lt, INPUT1, INPUT2, (INPUT1 < INPUT2))  \
  CHECK_OP(TYPE, Ne, INPUT1, INPUT2, (INPUT1 != INPUT2))

  CHECK_ALL_COMPARISONS(Integer, 10, 20);
  CHECK_ALL_COMPARISONS(Integer, -10, 20);
  CHECK_ALL_COMPARISONS(Integer, 0, 0);
  CHECK_ALL_COMPARISONS(Real, 0.0, 0.0);
  CHECK_ALL_COMPARISONS(Real, 1.0, 0.0);
  CHECK_ALL_COMPARISONS(Real, -1.0, 0.0);
  CHECK_ALL_COMPARISONS(Real, 1.0, -2.0);
  CHECK_ALL_COMPARISONS(BoolVal, false, false);
  CHECK_ALL_COMPARISONS(BoolVal, true, false);
  CHECK_ALL_COMPARISONS(BoolVal, false, true);
  CHECK_ALL_COMPARISONS(BoolVal, true, true);

#undef CHECK_ALL_COMPARISONS
#undef CHECK_NULL
}

}  // namespace tpl::sql::test
