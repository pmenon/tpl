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
  const char *raw_string = "I only love my bed and my momma, I'm sorry";

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

TEST_F(StringFunctionsTests, SplitPartTests) {
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
    auto x = StringVal(raw_string);
    auto result = StringVal("");
    auto delim = StringVal(" ");
    auto field = Integer(30);
    StringFunctions::SplitPart(ctx(), &result, x, delim, field);
    EXPECT_FALSE(result.is_null);
    EXPECT_TRUE(StringVal("") == result)
        << "Expected empty string, got \""
        << std::string(reinterpret_cast<char *>(result.ptr), result.len)
        << "\"";
  }

  // Empty delimiter
  {
    auto x = StringVal(raw_string);
    auto result = StringVal("");
    auto delim = StringVal("");
    auto field = Integer(3);
    StringFunctions::SplitPart(ctx(), &result, x, delim, field);
    EXPECT_FALSE(result.is_null);
    EXPECT_TRUE(x == result);
  }

  auto x = StringVal(raw_string);
  auto result = StringVal("");

  // Valid
  {
    const char *delim = " ";
    auto s = llvm::StringRef(raw_string);

    llvm::SmallVector<llvm::StringRef, 4> splits;
    s.split(splits, delim);

    for (u32 i = 0; i < splits.size(); i++) {
      StringFunctions::SplitPart(ctx(), &result, x, StringVal(delim),
                                 Integer(i + 1));
      EXPECT_FALSE(result.is_null);
      auto split = splits[i].str();
      EXPECT_TRUE(StringVal(split.c_str()) == result)
          << "Expected \"" << splits[i].str() << "\", got \""
          << std::string(reinterpret_cast<char *>(result.ptr), result.len)
          << "\"";
    }
  }
}

}  // namespace tpl::sql::test
