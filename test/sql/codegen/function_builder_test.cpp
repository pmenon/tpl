#include "util/test_harness.h"

#include "ast/ast.h"
#include "ast/ast_pretty_print.h"
#include "ast/context.h"
#include "sema/error_reporter.h"
#include "sql/codegen/code_container.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/function_builder.h"
#include "sql/codegen/if.h"

namespace tpl::sql::codegen {

class FunctionBuilderTest : public TplTest {
 public:
  FunctionBuilderTest() : reporter_(), ctx_(&reporter_) {}

 protected:
  ast::Context *Ctx() { return &ctx_; }

 private:
  sema::ErrorReporter reporter_;
  ast::Context ctx_;
};

TEST_F(FunctionBuilderTest, Simple) {
  CodeContainer cc(Ctx());
  CodeGen codegen(Ctx());

  auto name = codegen.MakeIdentifier("temp");
  auto args = codegen.MakeFieldList(
      {codegen.MakeField(codegen.MakeIdentifier("arg_1"), codegen.Int16Type())});
  FunctionBuilder builder(&codegen, name, std::move(args), codegen.Int16Type());
  {
    auto arg = builder.GetParameterByPosition(0);
    auto x = codegen.BinaryOp(parsing::Token::Type::PLUS, arg, codegen.Const16(2));

    If a_lt_arg(&codegen, codegen.BinaryOp(parsing::Token::Type::LESS, x, codegen.Const16(2)));
    { builder.Append(codegen.BinaryOp(parsing::Token::Type::MINUS, x, codegen.Const16(2))); }
    a_lt_arg.Else();
    {
      If b_lt_arg(&codegen,
                  codegen.BinaryOp(parsing::Token::Type::GREATER_EQUAL, x, codegen.Const16(10)));
      {}
      b_lt_arg.Else();
      {}
      b_lt_arg.EndIf();
    }
    a_lt_arg.EndIf();
    builder.Finish();
  }
  ast::FunctionDecl *func = builder.GetConstructedFunction();
  ast::AstPrettyPrint::Dump(std::cout, func);
}

}  // namespace tpl::sql::codegen
