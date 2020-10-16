#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

/**
 * CaseExpression represents a SQL WHEN ... THEN ... statement.
 */
class CaseExpression : public AbstractExpression {
 public:
  /** WHEN ... THEN ... clauses. */
  struct WhenClause {
    // The condition to be checked for this case expression.
    std::unique_ptr<AbstractExpression> condition;
    // The value to produce if the corresponding condition is true.
    std::unique_ptr<AbstractExpression> then;
  };

  /**
   * Instantiate a new case expression.
   * @param return_value_type return value of the case expression
   * @param when_clauses list of WhenClauses
   * @param default_expr default expression for this case
   */
  CaseExpression(const TypeId return_value_type, std::vector<WhenClause> &&when_clauses,
                 std::unique_ptr<AbstractExpression> default_expr)
      : AbstractExpression(ExpressionType::OPERATOR_CASE_EXPR, return_value_type, {}),
        when_clauses_(std::move(when_clauses)),
        default_expr_(std::move(default_expr)) {}

  /**
   * @return The number of clauses in the case expression.
   */
  size_t GetWhenClauseSize() const { return when_clauses_.size(); }

  /**
   * @return The condition for the clause at the given index.
   */
  AbstractExpression *GetWhenClauseCondition(std::size_t index) const {
    TPL_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return when_clauses_[index].condition.get();
  }

  /**
   * @return The value of the clause at the given index.
   */
  AbstractExpression *GetWhenClauseResult(std::size_t index) const {
    TPL_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return when_clauses_[index].then.get();
  }

  /**
   * @return The default clause; null if one does not exist.
   * */
  AbstractExpression *GetDefaultClause() const { return default_expr_.get(); }

 private:
  // List of when-then clauses.
  std::vector<WhenClause> when_clauses_;
  // Default result case.
  std::unique_ptr<AbstractExpression> default_expr_;
};

}  // namespace tpl::sql::planner
