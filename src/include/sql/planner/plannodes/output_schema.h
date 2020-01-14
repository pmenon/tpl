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
     * @param type The SQL type for this column.
     * @param nullable Is column nullable.
     * @param expr The expression used to generate this column.
     */
    Column(const sql::TypeId type, const bool nullable, const planner::AbstractExpression *expr)
        : type_(type), nullable_(nullable), expr_(expr) {}

    /**
     * Default constructor used for deserialization.
     */
    Column() = default;

    /**
     * @return The SQL type for this column.
     */
    sql::TypeId GetType() const { return type_; }

    /**
     * @return True if the column is nullable, false otherwise.
     */
    bool GetNullable() const { return nullable_; }

    /**
     * @return The expression used to produce the value of the column.
     */
    const planner::AbstractExpression *GetExpr() const { return expr_; }

   private:
    // SQL type.
    sql::TypeId type_;
    // NULL-able flag.
    bool nullable_;
    // Producing expression.
    const planner::AbstractExpression *expr_;
  };

  /**
   * A builder of output schema.
   */
  class Builder {
   public:
    /**
     * Append an output a column to the schema.
     * @param col The column to add.
     * @return This builder.
     */
    Builder &AddColumn(OutputSchema::Column &&col) {
      cols_.emplace_back(col);
      return *this;
    }

    /**
     * Append an output column with the given type, NULL-able flag and producing expression to this
     * output schema. The column is added to the end of the list of columns.
     * @param type The SQL type of the column.
     * @param nullable The NULL-able flag.
     * @param expr The producing expression.
     * @return This builder.
     */
    Builder &AddColumn(const sql::TypeId type, const bool nullable,
                       const planner::AbstractExpression *expr) {
      return AddColumn(OutputSchema::Column(type, nullable, expr));
    }

    /**
     *
     * @return
     */
    std::unique_ptr<OutputSchema> Build() {
      return std::make_unique<OutputSchema>(std::move(cols_));
    }

   private:
    std::vector<Column> cols_;
  };

  /**
   * Instantiates a OutputSchema.
   * @param columns The collection of columns making up the schema.
   */
  explicit OutputSchema(std::vector<Column> columns) : columns_(std::move(columns)) {}

  /**
   * Copy constructs an OutputSchema.
   * @param other the OutputSchema to be copied.
   */
  OutputSchema(const OutputSchema &other) = default;

  /**
   * Default constructor for deserialization.
   */
  OutputSchema() = default;

  /**
   * @param col_idx offset into the schema specifying which Column to access.
   * @return description of the schema for a specific column.
   */
  Column GetColumn(const uint32_t col_idx) const {
    TPL_ASSERT(col_idx < columns_.size(), "column id is out of bounds for this Schema");
    return columns_[col_idx];
  }
  /**
   * @return the vector of columns that are part of this schema.
   */
  const std::vector<Column> &GetColumns() const { return columns_; }

 private:
  // The columns.
  std::vector<Column> columns_;
};

}  // namespace tpl::sql::planner
