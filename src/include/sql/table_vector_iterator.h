#pragma once

#include <vector>
#include "storage/sql_table.h"

#include "sql/projected_columns_iterator.h"

namespace tpl::sql {
using namespace terrier;
/// An iterator over a table's data in vector-wise fashion
class TableVectorIterator {
 public:
  /// Create a new vectorized iterator over the given table
  explicit TableVectorIterator(
      const terrier::storage::SqlTable &table,
      const terrier::catalog::Schema &schema,
      terrier::transaction::TransactionContext *txn = nullptr);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(TableVectorIterator);

  /// Advance the iterator by a vector of input
  /// \return True if there is more data in the iterator; false otherwise
  bool Advance();

  /// Return the iterator over the current active ProjectedColumns
  ProjectedColumnsIterator *vector_projection_iterator() {
    return &vector_projection_iterator_;
  }

 private:
  /**
   * Helper method to get the PC initializer.
   * @param table SqlTable we want to iterate over.
   * @param schema Schema of the table
   * @return the initialization pait.
   */
  static std::pair<storage::ProjectedColumnsInitializer, storage::ProjectionMap>
  GetInitializer(const storage::SqlTable &table, const catalog::Schema &schema);

  // SqlTable to iterate over
  const terrier::storage::SqlTable &table_;

  // Schema of the table
  const terrier::catalog::Schema &schema_;

  // Transaction trying to iterate over the table
  terrier::transaction::TransactionContext *txn_;

  // A PC and its buffer of the PC.
  byte *buffer_;
  terrier::storage::ProjectedColumns *projected_columns_;

  // The initilization pair for the PC.
  std::pair<terrier::storage::ProjectedColumnsInitializer,
            terrier::storage::ProjectionMap>
      initializer_;

  // Iterator of the slots in the PC
  storage::DataTable::SlotIterator iter_;

  // Whether no transaction was passed in.
  bool null_txn_;

  // An iterator over the currently active projection
  ProjectedColumnsIterator vector_projection_iterator_;
};

}  // namespace tpl::sql
