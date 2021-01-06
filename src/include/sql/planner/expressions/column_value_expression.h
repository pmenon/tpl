#pragma once

#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

/**
 * Represents a column tuple value.
 */
class ColumnValueExpression : public AbstractExpression {
 public:
  ColumnValueExpression(uint16_t column_oid, const Type &type)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type, {}), column_oid_(column_oid) {}

  /** @return The ID of the column this expression refers to. */
  uint16_t GetColumnOid() const { return column_oid_; }

 private:
  // OID of the column.
  uint16_t column_oid_;
};

}  // namespace tpl::sql::planner
