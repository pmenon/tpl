#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "llvm/ADT/StringRef.h"

#include "sql/execution_context.h"
#include "sql/functions/string_functions.h"
#include "sql/value.h"
#include "util/timer.h"

namespace tpl::sql::test {

class StringFunctionsTests : public TplTest {
 public:
  StringFunctionsTests() : ctx_(nullptr) {}

  ExecutionContext *ctx() { return &ctx_; }

 protected:
  const char *test_string_1 = "I only love my bed and my momma, I'm sorry";
  const char *test_string_2 = "Drake";

 private:
  ExecutionContext ctx_;
};

TEST_F(StringFunctionsTests, Substring) {
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
  auto x = StringVal(test_string_1);
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
    EXPECT_TRUE(StringVal("") == result);
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

TEST_F(StringFunctionsTests, SplitPart) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto delim = StringVal("");
    auto field = Integer(0);

    StringFunctions::SplitPart(ctx(), &result, x, delim, field);
    EXPECT_TRUE(result.is_null);

    result = StringVal("");
    StringFunctions::SplitPart(ctx(), &result, x, delim, field);
    EXPECT_TRUE(result.is_null);
  }

  // Negative field
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto delim = StringVal("");
    auto field = Integer(-30);
    StringFunctions::SplitPart(ctx(), &result, x, delim, field);
    EXPECT_TRUE(result.is_null);
  }

  // Invalid field
  {
    auto x = StringVal(test_string_1);
    auto result = StringVal("");
    auto delim = StringVal(" ");
    auto field = Integer(30);
    StringFunctions::SplitPart(ctx(), &result, x, delim, field);
    EXPECT_TRUE(StringVal("") == result);
  }

  // Empty delimiter
  {
    auto x = StringVal(test_string_1);
    auto result = StringVal("");
    auto delim = StringVal("");
    auto field = Integer(3);
    StringFunctions::SplitPart(ctx(), &result, x, delim, field);
    EXPECT_TRUE(x == result);
  }

  auto x = StringVal(test_string_1);
  auto result = StringVal("");

  // Valid
  {
    const char *delim = " ";
    auto s = llvm::StringRef(test_string_1);

    llvm::SmallVector<llvm::StringRef, 4> splits;
    s.split(splits, delim);

    for (u32 i = 0; i < splits.size(); i++) {
      StringFunctions::SplitPart(ctx(), &result, x, StringVal(delim),
                                 Integer(i + 1));
      auto split = splits[i].str();
      EXPECT_TRUE(StringVal(split.c_str()) == result);
    }
  }
}

TEST_F(StringFunctionsTests, Repeat) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto n = Integer(0);

    StringFunctions::Repeat(ctx(), &result, x, n);
    EXPECT_TRUE(result.is_null);

    x = StringVal(test_string_2);
    result = StringVal("");
    n = Integer::Null();

    StringFunctions::Repeat(ctx(), &result, x, n);
    EXPECT_TRUE(result.is_null);
  }

  auto x = StringVal(test_string_2);
  auto result = StringVal("");
  auto n = Integer(0);

  // n = 0, expect empty result
  StringFunctions::Repeat(ctx(), &result, x, n);
  EXPECT_TRUE(StringVal("") == result);

  // n = -1, expect empty
  n = Integer(-1);
  StringFunctions::Repeat(ctx(), &result, x, n);
  EXPECT_TRUE(StringVal("") == result);

  // n = 1, expect original back
  n = Integer(1);
  StringFunctions::Repeat(ctx(), &result, x, n);
  EXPECT_TRUE(x == result);

  // n = 4, expect four copies
  const auto repeats = 4;

  std::string s;
  for (auto i = 0; i < repeats; i++) s += test_string_2;

  n = Integer(repeats);
  StringFunctions::Repeat(ctx(), &result, x, n);
  EXPECT_FALSE(result.is_null);
  EXPECT_TRUE(StringVal(s.c_str()) == result);
}

TEST_F(StringFunctionsTests, Lpad) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto len = Integer(0);
    auto pad = StringVal("");

    StringFunctions::Lpad(ctx(), &result, x, len, pad);
    EXPECT_TRUE(result.is_null);
  }

  // No work
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(4);
    auto pad = StringVal("");

    StringFunctions::Lpad(ctx(), &result, x, len, pad);
    EXPECT_TRUE(x == result);
  }

  // Trim
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(2);
    auto pad = StringVal("");

    StringFunctions::Lpad(ctx(), &result, x, len, pad);
    EXPECT_TRUE(StringVal("te") == result);
  }

  auto x = StringVal("hi");
  auto result = StringVal("");
  auto len = Integer(5);
  auto pad = StringVal("xy");

  StringFunctions::Lpad(ctx(), &result, x, len, pad);
  EXPECT_TRUE(StringVal("xyxhi") == result);
}

TEST_F(StringFunctionsTests, Rpad) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto len = Integer(0);
    auto pad = StringVal("");

    StringFunctions::Lpad(ctx(), &result, x, len, pad);
    EXPECT_TRUE(result.is_null);
  }

  // No work
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(4);
    auto pad = StringVal("");

    StringFunctions::Lpad(ctx(), &result, x, len, pad);
    EXPECT_TRUE(x == result);
  }

  // Trim
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(2);
    auto pad = StringVal("");

    StringFunctions::Lpad(ctx(), &result, x, len, pad);
    EXPECT_TRUE(StringVal("te") == result);
  }

  auto x = StringVal("hi");
  auto result = StringVal("");
  auto len = Integer(5);
  auto pad = StringVal("xy");

  StringFunctions::Rpad(ctx(), &result, x, len, pad);
  EXPECT_TRUE(StringVal("hixyx") == result);
}

TEST_F(StringFunctionsTests, Lower) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");

    StringFunctions::Lower(ctx(), &result, x);
    EXPECT_TRUE(result.is_null);
  }

  auto x = StringVal("TEST");
  auto result = StringVal("");
  StringFunctions::Lower(ctx(), &result, x);
  EXPECT_TRUE(StringVal("test") == result);
}

TEST_F(StringFunctionsTests, Upper) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");

    StringFunctions::Upper(ctx(), &result, x);
    EXPECT_TRUE(result.is_null);
  }

  auto x = StringVal("test");
  auto result = StringVal("");
  StringFunctions::Upper(ctx(), &result, x);
  EXPECT_TRUE(StringVal("TEST") == result);
}

}  // namespace tpl::sql::test
