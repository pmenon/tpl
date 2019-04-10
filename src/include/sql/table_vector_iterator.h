#pragma once

#include <vector>
#include "storage/sql_table.h"

#include "sql/vector_projection_iterator.h"

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
  VectorProjectionIterator *vector_projection_iterator() {
    return &vector_projection_iterator_;
  }

 private:
  static std::pair<storage::ProjectedColumnsInitializer, storage::ProjectionMap>
  GetInitializer(const storage::SqlTable &table, const catalog::Schema &schema);

  const terrier::storage::SqlTable &table_;
  const terrier::catalog::Schema &schema_;
  terrier::transaction::TransactionContext *txn_;
  byte *buffer_;
  terrier::storage::ProjectedColumns *projected_columns_;
  std::pair<terrier::storage::ProjectedColumnsInitializer,
            terrier::storage::ProjectionMap>
      initializer_;
  storage::DataTable::SlotIterator iter_;
  bool first_call_;

  // An iterator over the currently active projection
  VectorProjectionIterator vector_projection_iterator_;
};

}  // namespace tpl::sql
