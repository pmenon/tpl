#pragma once

#include <utility>
#include <vector>

#include "sql/planner/expressions/expression_defs.h"
#include "sql/sql.h"

namespace tpl::sql::planner {

/**
 * An abstract parser expression. Dumb and immutable.
 */
class AbstractExpression {
 protected:
  /**
   * Instantiates a new abstract expression. Because these are logical expressions, everything
   * should be known at the time of instantiation, i.e. the resulting object is immutable.
   * @param expression_type what type of expression we have
   * @param return_value_type the type of the expression's value
   * @param children the list of children for this node
   */
  AbstractExpression(ExpressionType expression_type, TypeId return_value_type,
                     std::vector<const AbstractExpression *> &&children)
      : expression_type_(expression_type),
        return_value_type_(return_value_type),
        children_(std::move(children)) {}

  /**
   * Copy constructs an abstract expression.
   * @param other the abstract expression to be copied
   */
  AbstractExpression(const AbstractExpression &other) = default;

  /**
   * @param return_value_type Set the return value type of the current expression
   */
  void SetReturnValueType(TypeId return_value_type) { return_value_type_ = return_value_type; }

 public:
  virtual ~AbstractExpression() = default;

  /**
   * @return type of this expression
   */
  ExpressionType GetExpressionType() const { return expression_type_; }

  /**
   * @return type of the return value
   */
  TypeId GetReturnValueType() const { return return_value_type_; }

  /**
   * @return number of children in this abstract expression
   */
  size_t GetChildrenSize() const { return children_.size(); }

  /**
   * @return children of this abstract expression
   */
  const std::vector<const AbstractExpression *> &GetChildren() const { return children_; }

  /**
   * @param index index of child
   * @return child of abstract expression at that index
   */
  const AbstractExpression *GetChild(uint64_t index) const {
    TPL_ASSERT(index < children_.size(), "Index must be in bounds.");
    return children_[index];
  }

  virtual void DeriveReturnValueType() {}

 private:
  /**
   * Type of the current expression
   */
  ExpressionType expression_type_;

  /**
   * Type of the return value
   */
  TypeId return_value_type_;

  /**
   * List fo children expressions
   */
  std::vector<const AbstractExpression *> children_;
};

}  // namespace tpl::sql::planner
