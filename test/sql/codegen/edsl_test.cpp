#include "ast/context.h"
#include "sema/error_reporter.h"
#include "sql/codegen/edsl/arithmetic_ops.h"
#include "sql/codegen/edsl/comparison_ops.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/loop.h"
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
    edsl::Variable<int *> x(&codegen, "x");
    edsl::Variable<int> y(&codegen, "y"), yyy(&codegen, "yyy");
    edsl::Variable<int **> zz(&codegen, "zz");
    edsl::Variable<int[]> a(&codegen, "a");
    edsl::Variable<bool> b(&codegen, "b");
    edsl::Variable<uint64_t> hash(&codegen, "hash");
    edsl::Variable<ast::x::HashTableEntry *> entry(&codegen, "entry");
    edsl::Variable<ast::x::JoinHashTable *[2]> yy(&codegen, "yy");

    edsl::Declare(x);
    edsl::Declare(y);
    edsl::Declare(zz);
    edsl::Declare(yy);
    edsl::Declare(yyy);
    edsl::Declare(hash);
    edsl::Declare(a);
    edsl::Declare(b, y == yyy);
    edsl::Assign(*zz, x);

    //    edsl::Declare(entry, yy[0]->Allocate(hash));
    edsl::Assign(y, y + y - y * y / y % y);
    edsl::Assign(a[0], y);
#if 0
    edsl::UInt8 i(&codegen, "i", 2);
    edsl::UInt8 j(&codegen, "j", i + i);
    edsl::UInt8 k = j;
    edsl::UInt8 l(&codegen, "l", k * k * k * k);
    edsl::UInt32 m(&codegen, "m", Ctlz(l) + Cttz(l));
    edsl::Ptr<edsl::UInt8> ptr(&codegen, "ptr");
    ptr.Store(j);
    Loop loop(&func, i < j);
    {
      //
      j = ++i;
      j += 20;
      j += (i + 2 - 1);
      j -= (i * 2);
      j *= (i * 4);
      j |= (i * 8);
    }
#endif
  }
  func.Finish();

  cu.Compile();
}

}  // namespace tpl::sql::codegen
