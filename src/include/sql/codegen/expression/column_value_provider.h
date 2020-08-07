#pragma once

namespace tpl::sql::codegen {

class ConsumerContext;

class ColumnValueProvider {
 public:
  /**
   * @return the child's output at the given index
   */
  virtual ast::Expr *GetChildOutput(ConsumerContext *context, uint32_t child_idx,
                                    uint32_t attr_idx) const = 0;

  /**
   * @return an expression representing the value of the column with the given OID.
   */
  virtual ast::Expr *GetTableColumn(uint16_t col_oid) const = 0;
};

}  // namespace tpl::sql::codegen
