#pragma once

#include <utility>
#include <vector>

#include "ast/identifier.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/edsl/struct.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"
#include "sql/type.h"

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
  explicit CompactStorage(CodeGen *codegen, std::string_view name, const std::vector<Type> &schema);

  /**
   * Setup the storage to store rows with the given schema.
   * @param schema The schema of the rows to materialize.
   */
  void Setup(const std::vector<Type> &schema);

  /**
   * Write the value, @em val, into the attribute index @em index in the row pointed to by @em ptr.
   * @param ptr The pointer to the row's buffer space (i.e., NOT a pointer to where you think the
   *            attribute should be stored, but where the ROW's contents are stored).
   * @param index The index in the input schema whose attribute/column we're to store.
   * @param val The SQL value to store.
   */
  void WriteSQL(const edsl::ReferenceVT &ptr, uint32_t index, const edsl::ValueVT &val) const;

  /**
   * Read the value of the column/attribute at index @em index in the row pointed to by @em ptr.
   * @param ptr The pointer to the row's buffer space (i.e., NOT a pointer to where you think the
   *            attribute should be stored, but where the ROW's contents are stored).
   * @param index The index in the input schema whose attribute/column we're to read.
   * @return A pair storing the read value and NULL-indication flag.
   */
  edsl::ValueVT ReadSQL(const edsl::ReferenceVT &ptr, uint32_t index) const;

  /**
   *
   * @param ptr
   * @param val
   */
  void WritePrimitive(const edsl::ReferenceVT &ptr, uint32_t index, const edsl::ValueVT &val) const;

  /**
   * Read the raw primitive component of the SQL value.
   * @param ptr The pointer to the row's buffer space (i.e., NOT a pointer to where you think the
   *            attribute should be stored, but where the ROW's contents are stored).
   * @param index The index in the input schema whose attribute/column we're to read.
   * @return The value.
   */
  edsl::ValueVT ReadPrimitive(const edsl::ReferenceVT &ptr, uint32_t index) const;

  /**
   * @return The name of the constructed compact type.
   */
  ast::Identifier GetTypeName() const { return struct_.GetName(); }

  /**
   * @return The constructed type for the row.
   */
  ast::Type *GetType() const { return struct_.GetType(); }

  /**
   * @return A type representing a pointer to the constructed type for the row.
   */
  ast::Type *GetPtrToType() const { return struct_.GetPtrToType(); }

  /**
   * @return The size of the construct type, in bytes.
   */
  edsl::Value<uint32_t> GetTypeSize() const { return struct_.GetSize(); }

 private:
  // Code generation instance.
  CodeGen *codegen_;
  // The types of each column.
  std::vector<Type> col_types_;
  // The compact structure.
  edsl::Struct struct_;
};

}  // namespace tpl::sql::codegen
