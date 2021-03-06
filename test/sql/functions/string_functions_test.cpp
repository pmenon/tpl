#include <string>
#include <vector>

#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#include "sql/execution_context.h"
#include "sql/functions/string_functions.h"
#include "sql/value.h"
#include "util/test_harness.h"
#include "util/timer.h"

namespace tpl::sql {

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

TEST_F(StringFunctionsTests, Concat) {
  // Nulls
  {
    auto null = StringVal::Null();
    auto result = StringVal("");
    const StringVal *inputs[] = {&null, &null};
    StringFunctions::Concat(&result, ctx(), inputs, sizeof(inputs) / sizeof(inputs[0]));
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(0u, result.GetLength());
  }

  {
    auto null = StringVal::Null();
    auto xy = StringVal("xy");
    auto result = StringVal("");

    const StringVal *inputs[] = {&null, &xy};
    StringFunctions::Concat(&result, ctx(), inputs, sizeof(inputs) / sizeof(inputs[0]));
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(xy, result);
  }

  {
    auto null = StringVal::Null();
    auto xy = StringVal("xy");
    auto result = StringVal("");

    const StringVal *inputs[] = {&xy, &null};
    StringFunctions::Concat(&result, ctx(), inputs, sizeof(inputs) / sizeof(inputs[0]));
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(xy, result);
  }

  // Simple Cases
  auto result = StringVal("");
  auto x = StringVal("xyz");
  auto a = StringVal("abc");
  auto null = StringVal::Null();

  {
    const StringVal *inputs[] = {&x, &a};
    StringFunctions::Concat(&result, ctx(), inputs, sizeof(inputs) / sizeof(inputs[0]));
    EXPECT_TRUE(StringVal("xyzabc") == result);
  }

  {
    const StringVal *inputs[] = {&x, &null, &a};
    StringFunctions::Concat(&result, ctx(), inputs, sizeof(inputs) / sizeof(inputs[0]));
    EXPECT_TRUE(StringVal("xyzabc") == result);
  }

  {
    const StringVal *inputs[] = {&a, &x, &null};
    StringFunctions::Concat(&result, ctx(), inputs, sizeof(inputs) / sizeof(inputs[0]));
    EXPECT_TRUE(StringVal("abcxyz") == result);
  }
}

TEST_F(StringFunctionsTests, Substring) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto pos = Integer(0);
    auto len = Integer(0);

    StringFunctions::Substring(&result, ctx(), x, pos);
    EXPECT_TRUE(result.is_null);

    result = StringVal("");
    StringFunctions::Substring(&result, ctx(), x, pos, len);
    EXPECT_TRUE(result.is_null);
  }

  // Checks
  auto x = StringVal(test_string_1);
  auto result = StringVal("");

  // Valid range
  {
    auto pos = Integer(3);
    auto len = Integer(4);
    StringFunctions::Substring(&result, ctx(), x, pos, len);
    EXPECT_TRUE(StringVal("only") == result);
  }

  // Negative position should return empty string
  {
    auto pos = Integer(-3);
    auto len = Integer(4);
    StringFunctions::Substring(&result, ctx(), x, pos, len);
    EXPECT_TRUE(StringVal("") == result);
  }

  // Negative length is null
  {
    auto pos = Integer(1);
    auto len = Integer(-1);
    StringFunctions::Substring(&result, ctx(), x, pos, len);
    EXPECT_TRUE(result.is_null);
  }

  // Negative length is null
  {
    auto pos = Integer(1);
    auto len = Integer(-1);
    StringFunctions::Substring(&result, ctx(), x, pos, len);
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

    StringFunctions::SplitPart(&result, ctx(), x, delim, field);
    EXPECT_TRUE(result.is_null);

    result = StringVal("");
    StringFunctions::SplitPart(&result, ctx(), x, delim, field);
    EXPECT_TRUE(result.is_null);
  }

  // Negative field
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto delim = StringVal("");
    auto field = Integer(-30);
    StringFunctions::SplitPart(&result, ctx(), x, delim, field);
    EXPECT_TRUE(result.is_null);
  }

  // Invalid field
  {
    auto x = StringVal(test_string_1);
    auto result = StringVal("");
    auto delim = StringVal(" ");
    auto field = Integer(30);
    StringFunctions::SplitPart(&result, ctx(), x, delim, field);
    EXPECT_TRUE(StringVal("") == result);
  }

  // Empty delimiter
  {
    auto x = StringVal(test_string_1);
    auto result = StringVal("");
    auto delim = StringVal("");
    auto field = Integer(3);
    StringFunctions::SplitPart(&result, ctx(), x, delim, field);
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

    for (uint32_t i = 0; i < splits.size(); i++) {
      StringFunctions::SplitPart(&result, ctx(), x, StringVal(delim), Integer(i + 1));
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

    StringFunctions::Repeat(&result, ctx(), x, n);
    EXPECT_TRUE(result.is_null);

    x = StringVal(test_string_2);
    result = StringVal("");
    n = Integer::Null();

    StringFunctions::Repeat(&result, ctx(), x, n);
    EXPECT_TRUE(result.is_null);
  }

  auto x = StringVal(test_string_2);
  auto result = StringVal("");
  auto n = Integer(0);

  // n = 0, expect empty result
  StringFunctions::Repeat(&result, ctx(), x, n);
  EXPECT_TRUE(StringVal("") == result);

  // n = -1, expect empty
  n = Integer(-1);
  StringFunctions::Repeat(&result, ctx(), x, n);
  EXPECT_TRUE(StringVal("") == result);

  // n = 1, expect original back
  n = Integer(1);
  StringFunctions::Repeat(&result, ctx(), x, n);
  EXPECT_TRUE(x == result);

  // n = 4, expect four copies
  const auto repeats = 4;

  std::string s;
  for (auto i = 0; i < repeats; i++) s += test_string_2;

  n = Integer(repeats);
  StringFunctions::Repeat(&result, ctx(), x, n);
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

    StringFunctions::Lpad(&result, ctx(), x, len, pad);
    EXPECT_TRUE(result.is_null);
  }

  // No work
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(4);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, ctx(), x, len, pad);
    EXPECT_TRUE(x == result);
  }

  // Trim
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(2);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, ctx(), x, len, pad);
    EXPECT_TRUE(StringVal("te") == result);
  }

  auto x = StringVal("hi");
  auto result = StringVal("");
  auto len = Integer(5);
  auto pad = StringVal("xy");

  StringFunctions::Lpad(&result, ctx(), x, len, pad);
  EXPECT_TRUE(StringVal("xyxhi") == result);
}

TEST_F(StringFunctionsTests, Rpad) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto len = Integer(0);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, ctx(), x, len, pad);
    EXPECT_TRUE(result.is_null);
  }

  // No work
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(4);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, ctx(), x, len, pad);
    EXPECT_TRUE(x == result);
  }

  // Trim
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(2);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, ctx(), x, len, pad);
    EXPECT_TRUE(StringVal("te") == result);
  }

  auto x = StringVal("hi");
  auto result = StringVal("");
  auto len = Integer(5);
  auto pad = StringVal("xy");

  StringFunctions::Rpad(&result, ctx(), x, len, pad);
  EXPECT_TRUE(StringVal("hixyx") == result);
}

TEST_F(StringFunctionsTests, Lower) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");

    StringFunctions::Lower(&result, ctx(), x);
    EXPECT_TRUE(result.is_null);
  }

  auto x = StringVal("TEST");
  auto result = StringVal("");
  StringFunctions::Lower(&result, ctx(), x);
  EXPECT_TRUE(StringVal("test") == result);
}

TEST_F(StringFunctionsTests, Upper) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");

    StringFunctions::Upper(&result, ctx(), x);
    EXPECT_TRUE(result.is_null);
  }

  auto x = StringVal("test");
  auto result = StringVal("");
  StringFunctions::Upper(&result, ctx(), x);
  EXPECT_TRUE(StringVal("TEST") == result);
}

TEST_F(StringFunctionsTests, Reverse) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");

    StringFunctions::Upper(&result, ctx(), x);
    EXPECT_TRUE(result.is_null);
  }

  // Empty
  {
    auto x = StringVal("");
    auto result = StringVal("");

    StringFunctions::Upper(&result, ctx(), x);
    EXPECT_TRUE(x == result);
  }

  auto x = StringVal("test");
  auto result = StringVal("");
  StringFunctions::Reverse(&result, ctx(), x);
  EXPECT_TRUE(StringVal("tset") == result);
}

TEST_F(StringFunctionsTests, Left) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Left(&result, ctx(), StringVal::Null(), Integer::Null());
    EXPECT_TRUE(result.is_null);
  }

  // Positive length
  auto x = StringVal("abcde");
  auto n = Integer(2);
  auto result = StringVal("");
  StringFunctions::Left(&result, ctx(), x, n);
  EXPECT_TRUE(StringVal("ab") == result);

  // Negative length
  n = Integer(-2);
  result = StringVal("");
  StringFunctions::Left(&result, ctx(), x, n);
  EXPECT_TRUE(StringVal("abc") == result);

  // Large length
  n = Integer(10);
  result = StringVal("");
  StringFunctions::Left(&result, ctx(), x, n);
  EXPECT_TRUE(x == result);

  // Large negative length
  n = Integer(-10);
  result = StringVal("");
  StringFunctions::Left(&result, ctx(), x, n);
  EXPECT_TRUE(StringVal("") == result);
}

TEST_F(StringFunctionsTests, Right) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Right(&result, ctx(), StringVal::Null(), Integer::Null());
    EXPECT_TRUE(result.is_null);
  }

  // Positive length
  auto x = StringVal("abcde");
  auto n = Integer(2);
  auto result = StringVal("");
  StringFunctions::Right(&result, ctx(), x, n);
  EXPECT_TRUE(StringVal("de") == result);

  // Negative length
  n = Integer(-2);
  result = StringVal("");
  StringFunctions::Right(&result, ctx(), x, n);
  EXPECT_TRUE(StringVal("cde") == result);

  // Large length
  n = Integer(10);
  result = StringVal("");
  StringFunctions::Right(&result, ctx(), x, n);
  EXPECT_TRUE(x == result);

  // Large negative length
  n = Integer(-10);
  result = StringVal("");
  StringFunctions::Right(&result, ctx(), x, n);
  EXPECT_TRUE(StringVal("") == result);
}

TEST_F(StringFunctionsTests, Ltrim) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Ltrim(&result, ctx(), StringVal::Null());
    EXPECT_TRUE(result.is_null);

    StringFunctions::Ltrim(&result, ctx(), StringVal::Null(), StringVal("xy"));
    EXPECT_TRUE(result.is_null);
  }

  // Simple
  auto x = StringVal("zzzytest");
  auto chars = StringVal("xyz");
  auto result = StringVal("");
  StringFunctions::Ltrim(&result, ctx(), x, chars);
  EXPECT_TRUE(StringVal("test") == result);

  // Remove all
  x = StringVal("zzzyxyyz");
  chars = StringVal("xyz");
  StringFunctions::Ltrim(&result, ctx(), x, chars);
  EXPECT_TRUE(StringVal("") == result);

  // Remove spaces
  x = StringVal("  test");
  StringFunctions::Ltrim(&result, ctx(), x);
  EXPECT_TRUE(StringVal("test") == result);
}

TEST_F(StringFunctionsTests, Rtrim) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Rtrim(&result, ctx(), StringVal::Null());
    EXPECT_TRUE(result.is_null);

    StringFunctions::Rtrim(&result, ctx(), StringVal::Null(), StringVal("xy"));
    EXPECT_TRUE(result.is_null);
  }

  // Simple
  auto x = StringVal("testxxzx");
  auto chars = StringVal("xyz");
  auto result = StringVal("");
  StringFunctions::Rtrim(&result, ctx(), x, chars);
  EXPECT_TRUE(StringVal("test") == result);

  // Remove all
  x = StringVal("zzzyxyyz");
  chars = StringVal("xyz");
  StringFunctions::Rtrim(&result, ctx(), x, chars);
  EXPECT_TRUE(StringVal("") == result);

  // Remove spaces
  x = StringVal("test   ");
  StringFunctions::Rtrim(&result, ctx(), x);
  EXPECT_TRUE(StringVal("test") == result);
}

TEST_F(StringFunctionsTests, Trim) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Trim(&result, ctx(), StringVal::Null());
    EXPECT_TRUE(result.is_null);

    StringFunctions::Trim(&result, ctx(), StringVal::Null(), StringVal("xy"));
    EXPECT_TRUE(result.is_null);
  }

  // Simple
  auto x = StringVal("yxPrashanthxx");
  auto chars = StringVal("xyz");
  auto result = StringVal("");
  StringFunctions::Trim(&result, ctx(), x, chars);
  EXPECT_TRUE(StringVal("Prashanth") == result);

  // Remove all
  x = StringVal("zzzyxyyz");
  chars = StringVal("xyz");
  StringFunctions::Trim(&result, ctx(), x, chars);
  EXPECT_TRUE(StringVal("") == result);

  // Remove all, but one
  x = StringVal("zzzyXxyyz");
  chars = StringVal("xyz");
  StringFunctions::Trim(&result, ctx(), x, chars);
  EXPECT_TRUE(StringVal("X") == result);

  // Remove spaces
  x = StringVal("   test   ");
  StringFunctions::Trim(&result, ctx(), x);
  EXPECT_TRUE(StringVal("test") == result);
}

}  // namespace tpl::sql
