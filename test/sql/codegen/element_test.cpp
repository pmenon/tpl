#include "ast/context.h"
#include "sema/error_reporter.h"
#include "sql/codegen/edsl/elements.h"
#include "vm/module.h"

// Tests
#include "util/codegen_test_harness.h"

namespace tpl::sql::codegen {

class ElementTest : public CodegenBasedTest {
 public:
  ElementTest() : errors_(), ctx_(&errors_) {}

  ast::Context *GetContext() { return &ctx_; }

 private:
  sema::ErrorReporter errors_;
  ast::Context ctx_;
};

TEST_F(ElementTest, SimpleCheck) {
  CompilationUnit cu(GetContext(), "test");
  CodeGen codegen(&cu);

  FunctionBuilder func(&codegen, codegen.MakeIdentifier("test"), codegen.MakeEmptyFieldList(),
                       codegen.Nil());
  {
    edsl::UInt8 i(&codegen, "i", 2);
    edsl::UInt8 j(&codegen, "j", i + i);
    edsl::UInt8 k = j;
    edsl::UInt8 l(&codegen, "l", k * k * k * k);
    edsl::Ptr<edsl::UInt8> ptr(&codegen, "ptr");
    ptr.Store(j);
    edsl::While(&codegen, i < j, [&] {
      //
      j = ++i;
      j += 20;
      j += (i + 2 - 1);
      j -= (i * 2);
      j *= (i * 4);
      j |= (i * 8);
    });
  }
  func.Finish();

  cu.Compile();
}

}  // namespace tpl::sql::codegen
