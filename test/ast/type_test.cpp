#include <string>
#include <utility>

#include "tpl_test.h"  // NOLINT

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

  ast::AstContext &ctx() { return ctx_; }

  ast::Identifier Name(const std::string &s) { return ctx().GetIdentifier(s); }

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
          {Name("b"), ast::BoolType::Get(ctx())},
          {Name("i64"), ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Int64)},
          {Name("i8_1"), ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Int8)},
          {Name("i32"), ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Int32)},
          {Name("i8_2"), ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Int8)},
          {Name("i16"), ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Int16)},
          {Name("p"), ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Int64)->PointerTo()},
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

TEST_F(TypeTest, PrimitiveTypeCacheTest) {
  //
  // In any one AstContext, we must have a cache of types. First, check all the
  // integer types
  //

#define GEN_INT_TEST(Kind)                                             \
  {                                                                    \
    auto *type1 =                                                      \
        ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Kind); \
    auto *type2 =                                                      \
        ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Kind); \
    EXPECT_EQ(type1, type2)                                            \
        << "Received two different " #Kind " types from context";      \
  }
  GEN_INT_TEST(Int8);
  GEN_INT_TEST(Int16);
  GEN_INT_TEST(Int32);
  GEN_INT_TEST(Int64);
  GEN_INT_TEST(UInt8);
  GEN_INT_TEST(UInt16);
  GEN_INT_TEST(UInt32);
  GEN_INT_TEST(UInt64);
#undef GEN_INT_TEST

  //
  // Try the floating point types ...
  //

#define GEN_FLOAT_TEST(Kind)                                                   \
  {                                                                            \
    auto *type1 = ast::FloatType::Get(ctx(), ast::FloatType::FloatKind::Kind); \
    auto *type2 = ast::FloatType::Get(ctx(), ast::FloatType::FloatKind::Kind); \
    EXPECT_EQ(type1, type2)                                                    \
        << "Received two different " #Kind " types from context";              \
  }
  GEN_FLOAT_TEST(Float32)
  GEN_FLOAT_TEST(Float64)
#undef GEN_FLOAT_TEST

  //
  // Really simple types
  //

  EXPECT_EQ(BoolType::Get(ctx()), BoolType::Get(ctx()));
  EXPECT_EQ(NilType::Get(ctx()), NilType::Get(ctx()));
}

TEST_F(TypeTest, StructTypeCacheTest) {
  //
  // Create two structurally equivalent types and ensure only one struct
  // instantiation is created in the context
  //

  {
    auto *type1 = ast::StructType::Get(util::RegionVector<ast::Field>(
        {{Name("a"), ast::BoolType::Get(ctx())},
         {Name("i64"),
          ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Int64)}},
        region()));

    auto *type2 = ast::StructType::Get(util::RegionVector<ast::Field>(
        {{Name("a"), ast::BoolType::Get(ctx())},
         {Name("i64"),
          ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Int64)}},
        region()));

    EXPECT_EQ(type1, type2)
        << "Received two different pointers to same struct type";
  }

  //
  // Create two **DIFFERENT** structures and ensure they have different
  // instantiations in the context
  //

  {
    auto *type1 = ast::StructType::Get(util::RegionVector<ast::Field>(
        {{Name("a"), ast::BoolType::Get(ctx())}}, region()));

    auto *type2 = ast::StructType::Get(util::RegionVector<ast::Field>(
        {{Name("a"),
          ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Int64)}},
        region()));

    EXPECT_NE(type1, type2)
        << "Received two equivalent pointers for different struct types";
  }
}

TEST_F(TypeTest, PointerTypeCacheTest) {
  //
  // Pointer types should also be cached. Thus, two *i8 types should have
  // pointer equality in a given context
  //

#define GEN_INT_TEST(Kind)                                              \
  {                                                                     \
    auto *type1 = ast::PointerType::Get(                                \
        ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Kind)); \
    auto *type2 = ast::PointerType::Get(                                \
        ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Kind)); \
    EXPECT_EQ(type1, type2)                                             \
        << "Received two different *" #Kind " types from context";      \
  }
  GEN_INT_TEST(Int8);
  GEN_INT_TEST(Int16);
  GEN_INT_TEST(Int32);
  GEN_INT_TEST(Int64);
  GEN_INT_TEST(UInt8);
  GEN_INT_TEST(UInt16);
  GEN_INT_TEST(UInt32);
  GEN_INT_TEST(UInt64);
#undef GEN_INT_TEST

  //
  // Try to create a pointer to the same struct and ensure the they point to the
  // same type instance
  //
  {
    auto *struct_type = ast::StructType::Get(util::RegionVector<ast::Field>(
        {{Name("a"), ast::BoolType::Get(ctx())}}, region()));

    auto *ptr1 = ast::PointerType::Get(struct_type);
    auto *ptr2 = ast::PointerType::Get(struct_type);

    EXPECT_EQ(ptr1, ptr2);
  };
}

TEST_F(TypeTest, FunctionTypeCacheTest) {
  //
  // Check that even function types are cached in the context. In the first
  // test, both functions have type: (bool)->bool
  //

  {
    auto *type1 = ast::FunctionType::Get(
        util::RegionVector<ast::Field>({{Name("a"), ast::BoolType::Get(ctx())}},
                                       region()),
        ast::BoolType::Get(ctx()));

    auto *type2 = ast::FunctionType::Get(
        util::RegionVector<ast::Field>({{Name("a"), ast::BoolType::Get(ctx())}},
                                       region()),
        ast::BoolType::Get(ctx()));

    EXPECT_EQ(type1, type2);
  }

  //
  // In this test, the two functions have different types, and hence, should not
  // cache to the same function type instance. The first function has type:
  // (bool)->bool, but the second has type (int32)->int32
  //

  {
    // The first function has type: (bool)->bool
    auto *type1 = ast::FunctionType::Get(
        util::RegionVector<ast::Field>({{Name("a"), ast::BoolType::Get(ctx())}},
                                       region()),
        ast::BoolType::Get(ctx()));

    auto *int_type =
        ast::IntegerType::Get(ctx(), ast::IntegerType::IntKind::Int32);
    auto *type2 = ast::FunctionType::Get(
        util::RegionVector<ast::Field>({{Name("a"), int_type}}, region()),
        int_type);

    EXPECT_NE(type1, type2);
  }
}

}  // namespace tpl::ast::test
