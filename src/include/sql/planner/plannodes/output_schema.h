#pragma once

#include <utility>
#include <vector>

#include "common/macros.h"
#include "sql/planner/expressions/abstract_expression.h"
#include "sql/planner/plannodes/plan_node_defs.h"
#include "sql/sql.h"

namespace tpl::sql::planner {

class OutputSchema {
 public:
  /**
   * This object contains output columns of a plan node which a type along with an expression to
   * generate the column.
   */
  class Column {
   public:
    /**
     * Instantiates a Column object.
     * @param type SQL type for this column
     * @param nullable is column nullable
     * @param expr the expression used to generate this column
     */
    Column(const sql::TypeId type, const bool nullable, const planner::AbstractExpression *expr)
        : type_(type), nullable_(nullable), expr_(expr) {}

    /**
     * Default constructor used for deserialization
     */
    Column() = default;

    /**
     * @return SQL type for this column
     */
    sql::TypeId GetType() const { return type_; }

    /**
     * @return true if the column is nullable, false otherwise
     */
    bool GetNullable() const { return nullable_; }

    /**
     * @return the expression of this column.
     */
    const planner::AbstractExpression *GetExpr() const { return expr_; }

   private:
    sql::TypeId type_;
    bool nullable_;
    const planner::AbstractExpression *expr_;
  };

  /**
   * Instantiates a OutputSchema.
   * @param columns collection of columns
   */
  explicit OutputSchema(std::vector<Column> columns) : columns_(std::move(columns)) {}

  /**
   * Copy constructs an OutputSchema.
   * @param other the OutputSchema to be copied
   */
  OutputSchema(const OutputSchema &other) = default;

  /**
   * Default constructor for deserialization
   */
  OutputSchema() = default;

  /**
   * @param col_idx offset into the schema specifying which Column to access
   * @return description of the schema for a specific column
   */
  Column GetColumn(const uint32_t col_idx) const {
    TPL_ASSERT(col_idx < columns_.size(), "column id is out of bounds for this Schema");
    return columns_[col_idx];
  }
  /**
   * @return the vector of columns that are part of this schema
   */
  const std::vector<Column> &GetColumns() const { return columns_; }

 private:
  std::vector<Column> columns_;
};

}  // namespace tpl::sql::planner
