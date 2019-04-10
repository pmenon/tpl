#include <compiler/TextCompiler.h>
#include <parser/postgresparser.h>
#include <algorithm>
#include <functional>
#include <string>
#include <vector>

#include "tpl_test.h"  // NOLINT

namespace tpl::compiler::test {

typedef terrier::parser::AbstractExpression AbstractExpression;

terrier::parser::PostgresParser pgparser;

class CompilerTest : public TplTest {};

TEST_F(CompilerTest, TextCompilerPredicateTest) {
  auto stmt_list =
      pgparser.BuildParseTree("SELECT * FROM test_1 WHERE colA=(1-4+5);");
  auto &sql_stmt = stmt_list[0];
  auto select_stmt =
      reinterpret_cast<terrier::parser::SelectStatement *>(sql_stmt.get());
  auto expr = select_stmt->GetSelectCondition();
  EXPECT_EQ(expr->GetExpressionType(),
            terrier::parser::ExpressionType::COMPARE_EQUAL);

  TextCompiler compiler;
  std::string s = compiler.CompilePredicate(expr, "row");
  LOG_INFO(s)
  EXPECT_EQ(s, "(row.cola==((1-4)+5))");
}

}  // namespace tpl::compiler::test
