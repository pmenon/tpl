#pragma once

#include "sql/column_vector_iterator.h"
#include "sql/table.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

/// An iterator over a table's data in vector-wise fashion
class TableVectorIterator {
 public:
  /// Create a new vectorized iterator over the given table
  explicit TableVectorIterator(const Table &table);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(TableVectorIterator);

  /// Advance the iterator by a vector of input
  /// \return True if there is more data in the iterator; false otherwise
  bool Advance();

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  VectorProjectionIterator *vector_projection_iterator() {
    return &vector_projection_iterator_;
  }

 private:
  // When the column iterators receive new vectors of input, we need to
  // refresh the vector projection with new data too
  void RefreshVectorProjection();

 private:
  // The iterate over the blocks stored in the table
  Table::BlockIterator block_iterator_;

  // The vector-wise iterators over each column in the table
  std::vector<ColumnVectorIterator> column_iterators_;

  // The active vector projection
  VectorProjection vector_projection_;

  // An iterator over the currently active projection
  VectorProjectionIterator vector_projection_iterator_;
};

}  // namespace tpl::sql
