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

NEVER_INLINE void f(edsl::UInt8 &i) {
  for (int j = 0; j < 10; j++) {
    ++i;
  }
}

TEST_F(ElementTest, SimpleCheck) {
  CompilationUnit cu(GetContext(), "test");
  CodeGen codegen(&cu);

  FunctionBuilder func(&codegen, codegen.MakeIdentifier("test"), codegen.MakeEmptyFieldList(),
                       codegen.Nil());
  {
    edsl::UInt8 i(&codegen, "i", 128);
    edsl::UInt8 j(&codegen, "j", i + i);
    edsl::UInt8 k = j;
    edsl::UInt8 l(&codegen, "l", k * k * k * k);
    edsl::while_(&codegen, i < j, [&]{
      //
      j = ++i;
      f(i);
    });

    //    i = (i + j) << i;  // good
    // i = a;             // bad
    //  auto yy = a + b;  // good
    //  auto zz = i + a;  // bad
    //  auto zzz = i.Cast<Int16>() + a;  // good
  }
  func.Finish();

  cu.Compile();
}

}  // namespace tpl::sql::codegen
