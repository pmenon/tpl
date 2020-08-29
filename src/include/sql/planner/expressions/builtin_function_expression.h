#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "ast/builtins.h"
#include "sql/generic_value.h"
#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

/**
 * Represents a builtin function call.
 */
class BuiltinFunctionExpression : public AbstractExpression {
 public:
  /**
   * Instantiate a new constant value expression.
   * @param value value to be held
   */
  explicit BuiltinFunctionExpression(ast::Builtin builtin,
                                     std::vector<const AbstractExpression *> &&children,
                                     const sql::TypeId return_value_type)
      : AbstractExpression(ExpressionType::BUILTIN_FUNCTION, return_value_type,
                           std::move(children)),
        builtin_(builtin) {}

  ast::Builtin GetBuiltin() const { return builtin_; }

 private:
  ast::Builtin builtin_;
};

}  // namespace tpl::sql::planner
