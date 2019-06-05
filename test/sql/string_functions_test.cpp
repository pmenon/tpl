#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "sql/execution_context.h"
#include "sql/functions/string_functions.h"
#include "sql/value.h"
#include "util/timer.h"

namespace tpl::sql::test {

class StringFunctionsTests : public TplTest {
 public:
  StringFunctionsTests() : ctx_(nullptr) {}

  ExecutionContext *ctx() { return &ctx_; }

 private:
  ExecutionContext ctx_;
};

TEST_F(StringFunctionsTests, SubstringTests) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto pos = Integer(0);
    auto len = Integer(0);

    StringFunctions::Substring(ctx(), &result, x, pos);
    EXPECT_TRUE(result.is_null);

    result = StringVal("");
    StringFunctions::Substring(ctx(), &result, x, pos, len);
    EXPECT_TRUE(result.is_null);
  }

  // Checks
  const char *raw_string = "I only love my bed and my momma, I'm sorry";
  auto x = StringVal(raw_string);
  auto result = StringVal("");

  // Valid range
  {
    auto pos = Integer(3);
    auto len = Integer(4);
    StringFunctions::Substring(ctx(), &result, x, pos, len);
    EXPECT_TRUE(StringVal("only") == result);
  }

  // Negative position should return empty string
  {
    auto pos = Integer(-3);
    auto len = Integer(4);
    StringFunctions::Substring(ctx(), &result, x, pos, len);
    EXPECT_TRUE(StringVal("") == result)
        << "Expected empty string, got \""
        << std::string((char *)result.ptr, result.len) << "\"";  // NOLINT
  }

  // Negative length is null
  {
    auto pos = Integer(1);
    auto len = Integer(-1);
    StringFunctions::Substring(ctx(), &result, x, pos, len);
    EXPECT_TRUE(result.is_null);
  }

  // Negative length is null
  {
    auto pos = Integer(1);
    auto len = Integer(-1);
    StringFunctions::Substring(ctx(), &result, x, pos, len);
    EXPECT_TRUE(result.is_null);
  }
}

}  // namespace tpl::sql::test
