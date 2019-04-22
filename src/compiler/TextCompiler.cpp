//
// Created by Tanuj Nayak on 4/8/19.
//

#include "compiler/TextCompiler.h"
#include <parsing/token.h>
#include <plan_node/abstract_plan_node.h>
#include <plan_node/seq_scan_plan_node.h>
#include <util/macros.h>
#include <sstream>
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "parser/expression_defs.h"
#include "type/transient_value_peeker.h"

namespace tpl::compiler {

void TextCompiler::CompileAndRun(
    const std::shared_ptr<tpl::plan_node::AbstractPlanNode> plan) {
  switch (plan->GetPlanNodeType()) {
    case tpl::plan_node::PlanNodeType::SEQSCAN:
      const tpl::plan_node::SeqScanPlanNode *scanPlan =
          reinterpret_cast<const tpl::plan_node::SeqScanPlanNode *>(plan.get());
      std::string predicate =
          CompilePredicate(scanPlan->GetPredicate(), DEFAULT_ROW_NAME);
      std::string template_string = break;
    default:
      TPL_ASSERT(false, "UNSUPPORTED PLAN");
  }
}
// trying recursive
void TextCompiler::CompileSubPredicate(
    const std::shared_ptr<tpl::parser::AbstractExpression> expression,
    const std::string &rowName, std::stringstream *stream) {
  size_t numChildren = expression->GetChildrenSize();
  if (numChildren > 2) {
    TPL_ASSERT(false, "Expression not supported yet");
  }
  tpl::parser::ExpressionType type = expression->GetExpressionType();

  if (numChildren == 0) {
    std::string constVal;
    switch (type) {
      case tpl::parser::ExpressionType::VALUE_TUPLE: {
        *stream << rowName;
        *stream << parsing::Token::GetString(parsing::Token::Type::DOT);
        *stream << reinterpret_cast<tpl::parser::TupleValueExpression *>(
                       expression.get())
                       ->GetColumnName();
        break;
      }
      case tpl::parser::ExpressionType::VALUE_CONSTANT: {
        tpl::type::TransientValue val =
            reinterpret_cast<tpl::parser::ConstantValueExpression *>(
                expression.get())
                ->GetValue();
        tpl::type::TypeId t = val.Type();

        // need to figure out a better way to write each case
        if (t != tpl::type::TypeId::INTEGER) {
          TPL_ASSERT(false, "Only support integer constants for now");
        }

        *stream << tpl::type::TransientValuePeeker::PeekInteger(val);
        break;
      }
      case tpl::parser::ExpressionType::VALUE_NULL: {
        *stream << parsing::Token::GetString(parsing::Token::Type::NIL_VAL);
        break;
      }
      default:
        TPL_ASSERT(false, "Expression not supported");
    }
    return;
  }

  *stream << parsing::Token::GetString(parsing::Token::Type::LEFT_PAREN);
  std::string middleChar = "";

  if (numChildren == 1) {
    switch (type) {
      case tpl::parser::ExpressionType::OPERATOR_NOT:
        middleChar = parsing::Token::GetString(parsing::Token::Type::BANG);
      default:
        TPL_ASSERT(false, "Expression not supported");
    }
    CompileSubPredicate(expression->GetChild(0), rowName, stream);
  }
  if (numChildren == 2) {
    CompileSubPredicate(expression->GetChild(0), rowName, stream);
    switch (type) {
      case tpl::parser::ExpressionType::OPERATOR_PLUS: {
        middleChar = parsing::Token::GetString(parsing::Token::Type::PLUS);
        break;
      }
      case tpl::parser::ExpressionType::OPERATOR_MINUS: {
        middleChar = parsing::Token::GetString(parsing::Token::Type::MINUS);
        break;
      }
      case tpl::parser::ExpressionType::OPERATOR_MULTIPLY: {
        middleChar = parsing::Token::GetString(parsing::Token::Type::STAR);
        break;
      }
      case tpl::parser::ExpressionType::OPERATOR_DIVIDE: {
        middleChar = parsing::Token::GetString(parsing::Token::Type::SLASH);
        break;
      }
      // case terrier::parser::ExpressionType::OPERATOR_CONCAT:
      case tpl::parser::ExpressionType::OPERATOR_MOD: {
        middleChar = parsing::Token::GetString(parsing::Token::Type::PERCENT);
        break;
      }
      case tpl::parser::ExpressionType::COMPARE_EQUAL: {
        middleChar =
            parsing::Token::GetString(parsing::Token::Type::EQUAL_EQUAL);
        break;
      }
      case tpl::parser::ExpressionType::COMPARE_NOT_EQUAL: {
        middleChar =
            parsing::Token::GetString(parsing::Token::Type::BANG_EQUAL);
        break;
      }
      case tpl::parser::ExpressionType::COMPARE_LESS_THAN: {
        middleChar = parsing::Token::GetString(parsing::Token::Type::LESS);
        break;
      }
      case tpl::parser::ExpressionType::COMPARE_GREATER_THAN: {
        middleChar = parsing::Token::GetString(parsing::Token::Type::GREATER);
        break;
      }
      case tpl::parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO: {
        middleChar =
            parsing::Token::GetString(parsing::Token::Type::LESS_EQUAL);
        break;
      }
      case tpl::parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO: {
        middleChar =
            parsing::Token::GetString(parsing::Token::Type::GREATER_EQUAL);
        break;
      }
      default:
        TPL_ASSERT(false, "Expression not supported");
    }
    *stream << middleChar;
    CompileSubPredicate(expression->GetChild(1), rowName, stream);
  }
  *stream << parsing::Token::GetString(parsing::Token::Type::RIGHT_PAREN);
}

std::string TextCompiler::CompilePredicate(
    const std::shared_ptr<const tpl::parser::AbstractExpression> expression,
    const std::string &rowName) {
  std::stringstream stream;
  CompileSubPredicate(expression, rowName, &stream);
  return stream.str();
}

}  // namespace tpl::compiler
