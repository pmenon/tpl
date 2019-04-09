#include <algorithm>
#include <functional>
#include <string>
#include <vector>
#include <compiler/TextCompiler.h>

#include "tpl_test.h"  // NOLINT

#include "parsing/scanner.h"

namespace tpl::compiler::test {

typedef terrier::parser::AbstractExpression AbstractExpression;

class CompilerTest : public TplTest {};

TEST_F(CompilerTest, TextCompilerPredicateTest) {
  //AbstractExpression root;
  //TextCompiler compiler;
  //compiler.CompilePredicate()
  //for (unsigned i = 0; i < 10; i++) {
  //  EXPECT_EQ(Token::Type::EOS, scanner.Next());
  //}
}

}  // namespace tpl::parsing::test
