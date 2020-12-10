#pragma once

#include <utility>
#include <vector>

#include "ast/identifier.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/sql.h"

namespace tpl::sql::codegen {

// Forward declare.
class CodeGen;

class CompactStorage {
 public:
  /**
   * Empty.
   */
  CompactStorage(CodeGen *codegen, std::string_view name);

  /**
   * Create an instance for a row with the provided schema.
   * @param schema The schema of the row to be stored.
   */
  explicit CompactStorage(CodeGen *codegen, std::string_view name,
                          const std::vector<TypeId> &schema);

  /**
   * Setup the storage to store rows with the given schema.
   * @param schema The schema of the rows to materialize.
   */
  void Setup(const std::vector<TypeId> &schema);

  /**
   * @return The name of the constructed compact type.
   */
  ast::Identifier GetTypeName() const { return type_name_; }

  /**
   * Write the value, @em val, into the attribute index @em index in the row pointed to by @em ptr.
   * @param ptr The pointer to the row's buffer space (i.e., NOT a pointer to where you think the
   *            attribute should be stored, but where the ROW's contents are stored).
   * @param index The index in the input schema whose attribute/column we're to store.
   * @param val The SQL value to store.
   */
  void WriteSQL(ast::Expression *ptr, uint32_t index, ast::Expression *val) const;

  /**
   * Read the value of the column/attribute at index @em index in the row pointed to by @em ptr.
   * @param ptr The pointer to the row's buffer space (i.e., NOT a pointer to where you think the
   *            attribute should be stored, but where the ROW's contents are stored).
   * @param index The index in the input schema whose attribute/column we're to read.
   * @return A pair storing the read value and NULL-indication flag.
   */
  ast::Expression *ReadSQL(ast::Expression *ptr, uint32_t index) const;

  /**
   * Read the raw primitive component of the SQL value.
   * @param ptr The pointer to the row's buffer space (i.e., NOT a pointer to where you think the
   *            attribute should be stored, but where the ROW's contents are stored).
   * @param index The index in the input schema whose attribute/column we're to read.
   * @return The value.
   */
  ast::Expression *ReadPrimitive(ast::Expression *ptr, uint32_t index) const;

  /**
   * @return The name of the field at the given index in the compact struct.
   */
  ast::Identifier FieldNameAtIndex(uint32_t index) const;

 private:
  // Given a pointer to the storage space, return a pointer to the NULL
  // indications array.
  ast::Expression *Nulls(ast::Expression *ptr) const;

  // Given a pointer to the storage space and the index of the column
  // to access, return a pointer to the attribute's column data.
  ast::Expression *ColumnPtr(ast::Expression *ptr, uint32_t index) const;

 private:
  // Code generation instance.
  CodeGen *codegen_;
  // The name of the declared type.
  ast::Identifier type_name_;
  // The names of all the fields, excluding the nulls.
  std::vector<std::pair<TypeId, ast::Identifier>> col_info_;
  // The name of the null-indicator array field.
  ast::Identifier nulls_;
};

}  // namespace tpl::sql::codegen
