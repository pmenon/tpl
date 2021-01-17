#pragma once

#include <memory>
#include <string>
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
     * @param expr The expression used to generate this column.
     */
    explicit Column(const planner::AbstractExpression *expr) : expr_(expr) {
      TPL_ASSERT(expr != nullptr, "NULL expression provided for column.");
    }

    /**
     * Default constructor used for deserialization.
     */
    Column() = default;

    /**
     * @return The SQL type for this column.
     */
    const Type &GetType() const { return expr_->GetReturnValueType(); }

    /**
     * @return True if the column is nullable, false otherwise.
     */
    bool GetNullable() const { return expr_->HasNullableValue(); }

    /**
     * @return The expression used to produce the value of the column.
     */
    const planner::AbstractExpression *GetExpr() const { return expr_; }

   private:
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
     * @param expr The producing expression.
     * @return This builder.
     */
    Builder &AddColumn(const planner::AbstractExpression *expr) {
      return AddColumn(OutputSchema::Column(expr));
    }

    /**
     * @return The constructed output schema.
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
  explicit OutputSchema(std::vector<Column> columns);

  /**
   * @return The information + description for the column at the given index in the output.
   */
  Column GetColumn(const uint32_t col_idx) const;

  /**
   * @return the vector of columns that are part of this schema.
   */
  const std::vector<Column> &GetColumns() const;

  /**
   * @return The byte offsets of each column in the serialized output.
   */
  const std::vector<std::size_t> &GetColumnOffsets() const;

  /**
   * @return The total number of bytes needed for an output row.
   */
  std::size_t ComputeOutputRowSize() const;

  /**
   * @return The number of output columns.
   */
  uint32_t NumColumns() const;

  /**
   * @return A pretty printed version of this output schema.
   */
  std::string ToString() const;

 private:
  // The columns.
  std::vector<Column> columns_;
  // The byte offsets where each column exist in the output.
  std::vector<std::size_t> column_offsets_;
};

}  // namespace tpl::sql::planner
