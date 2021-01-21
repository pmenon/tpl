#include "ast/context.h"
#include "ast/type.h"
#include "ast/type_builder.h"
#include "sema/error_reporter.h"

// For test.
#include "util/test_harness.h"

namespace tpl::ast {

class TypePrinterTest : public TplTest {
 public:
  TypePrinterTest() : errors_(), ctx_(&errors_) {}

  Context *Ctx() { return &ctx_; }

  Identifier Name(std::string_view n) { return ctx_.GetIdentifier(n); }

 private:
  sema::ErrorReporter errors_;
  Context ctx_;
};

TEST_F(TypePrinterTest, BuiltinTypeTest) {
  EXPECT_EQ("int32", TypeBuilder<int32_t>::Get(Ctx())->ToString());
  EXPECT_EQ("AggregationHashTable", TypeBuilder<x::AggregationHashTable>::Get(Ctx())->ToString());
  EXPECT_EQ("DateVal", TypeBuilder<x::DateVal>::Get(Ctx())->ToString());
}

TEST_F(TypePrinterTest, ArrayTypeTest) {
  EXPECT_EQ("[*]TimestampVal", TypeBuilder<x::TimestampVal[]>::Get(Ctx())->ToString());
  EXPECT_EQ("[4]DateMinAggregate", TypeBuilder<x::DateMinAggregate[4]>::Get(Ctx())->ToString());
}

TEST_F(TypePrinterTest, StructTypeTest) {
  auto type = StructType::Get(Ctx(),
                              // Name.
                              Name("Test"),
                              // Fields.
                              util::RegionVector<Field>(
                                  {
                                      {Name("a"), TypeBuilder<uint32_t>::Get(Ctx())},
                                      {Name("b"), TypeBuilder<std::string>::Get(Ctx())},
                                      {Name("c"), TypeBuilder<x::JoinHashTable *>::Get(Ctx())},
                                      {Name("d"), TypeBuilder<x::Date[4]>::Get(Ctx())},
                                  },
                                  Ctx()->GetRegion()));
  EXPECT_EQ("struct{uint32,string,*JoinHashTable,[4]Date}", type->ToString());
}

TEST_F(TypePrinterTest, FunctionTypeTest) {
  auto type = TypeBuilder<int32_t()>::Get(Ctx());
  EXPECT_EQ("()->int32", type->ToString());
}

}  // namespace tpl::ast
