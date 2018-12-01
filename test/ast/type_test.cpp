#include "tpl_test.h"

#include "ast/ast_context.h"
#include "ast/type.h"
#include "sema/error_reporter.h"
#include "util/region.h"

namespace tpl::ast::test {

class TypeTest : public TplTest {
 public:
  TypeTest()
      : region_("ast_test"), errors_(&region_), ctx_(&region_, errors_) {}

  util::Region *region() { return &region_; }

  ast::AstContext *ctx() { return &ctx_; }

  ast::Identifier Name(const std::string &s) { return ctx()->GetIdentifier(s); }

 private:
  util::Region region_;
  sema::ErrorReporter errors_;
  ast::AstContext ctx_;
};

TEST_F(TypeTest, StructPaddingTest) {
  /**
   * Summary: We create a TPL struct with the same fields as the test structure
   * 'Test' below. We expect the sizes to be the exact same, and the offsets of
   * each field to be the same.  In essence, we want TPL's layout engine to
   * replicate C/C++.
   */

  struct Test {
    bool b;
    i64 _i64;
    i8 _i8_1;
    i32 _i32;
    i8 _i8_2;
    i16 _i16;
    i64 *p;
  };

  // clang-format off
  auto fields = util::RegionVector<ast::Field>(
      {
          {Name("b"), ast::BoolType::Get(*ctx())},
          {Name("i64"), ast::IntegerType::Get(*ctx(), ast::IntegerType::IntKind::Int64)},
          {Name("i8_1"), ast::IntegerType::Get(*ctx(), ast::IntegerType::IntKind::Int8)},
          {Name("i32"), ast::IntegerType::Get(*ctx(), ast::IntegerType::IntKind::Int32)},
          {Name("i8_2"), ast::IntegerType::Get(*ctx(), ast::IntegerType::IntKind::Int8)},
          {Name("i16"), ast::IntegerType::Get(*ctx(), ast::IntegerType::IntKind::Int16)},
          {Name("p"), ast::IntegerType::Get(*ctx(), ast::IntegerType::IntKind::Int64)->PointerTo()},
      },
      region());
  // clang-format on

  auto *type = ast::StructType::Get(std::move(fields));

  // Expect: [0-1] b, [2-7] pad, [8-15] i64, [16-17] i8_1, [18-19] pad,
  //         [20-23] i32, [24-25] i8_2, [26-27] i16, [28-31] pad, [32-40] p
  EXPECT_EQ(sizeof(Test), type->size());
  EXPECT_EQ(alignof(Test), type->alignment());
  EXPECT_EQ(offsetof(Test, b), type->GetOffsetOfFieldByName(Name("b")));
  EXPECT_EQ(offsetof(Test, _i64), type->GetOffsetOfFieldByName(Name("i64")));
  EXPECT_EQ(offsetof(Test, _i8_1), type->GetOffsetOfFieldByName(Name("i8_1")));
  EXPECT_EQ(offsetof(Test, _i32), type->GetOffsetOfFieldByName(Name("i32")));
  EXPECT_EQ(offsetof(Test, _i16), type->GetOffsetOfFieldByName(Name("i16")));
  EXPECT_EQ(offsetof(Test, _i8_2), type->GetOffsetOfFieldByName(Name("i8_2")));
  EXPECT_EQ(offsetof(Test, p), type->GetOffsetOfFieldByName(Name("p")));
}

}  // namespace tpl::ast::test