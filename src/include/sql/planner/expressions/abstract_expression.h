#pragma once

#include <utility>
#include <vector>

#include "sql/planner/expressions/expression_defs.h"
#include "sql/type.h"

namespace tpl::sql::planner {

/**
 * An abstract parser expression. Dumb and immutable.
 */
class AbstractExpression {
 protected:
  /**
   * Instantiates a new abstract expression. Because these are logical expressions, everything
   * should be known at the time of instantiation, i.e. the resulting object is immutable.
   * @param expression_type The type of expression we have.
   * @param return_value_type The type of the expression's value.
   * @param children The list of children for this node.
   */
  AbstractExpression(ExpressionType expression_type, Type return_value_type,
                     std::vector<const AbstractExpression *> &&children)
      : expression_type_(expression_type),
        return_value_type_(return_value_type),
        children_(std::move(children)) {}

  /**
   * Copy constructs an abstract expression.
   * @param other the abstract expression to be copied
   */
  AbstractExpression(const AbstractExpression &other) = default;

  /** @param return_value_type Set the return value type of the current expression */
  void SetReturnValueType(const Type &return_value_type) { return_value_type_ = return_value_type; }

 public:
  /** Destructor. */
  virtual ~AbstractExpression() = default;

  /** * @return The type of this expression. */
  ExpressionType GetExpressionType() const { return expression_type_; }

  /** @return True if this expression is of the provided template type; false otherwise. */
  template <ExpressionType Type>
  bool Is() const noexcept {
    return expression_type_ == Type;
  }

  /** @return The SQL type of value this expression produces and returns. */
  const Type &GetReturnValueType() const { return return_value_type_; }

  /** @return True if the return type is NULL-able; false otherwise. */
  bool HasNullableValue() const { return return_value_type_.IsNullable(); }

  /** @return The number of children the expression has. */
  std::size_t NumChildren() const { return children_.size(); }

  /** @return The n-th child of this expression. */
  const AbstractExpression *GetChild(uint64_t index) const {
    TPL_ASSERT(index < children_.size(), "Index must be in bounds.");
    return children_[index];
  }

 private:
  // The expression type.
  ExpressionType expression_type_;
  // The expression's return type.
  Type return_value_type_;
  // List fo children expressions.
  std::vector<const AbstractExpression *> children_;
};

}  // namespace tpl::sql::planner
