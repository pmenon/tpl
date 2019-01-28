#pragma once

#include "sql/schema.h"

namespace tpl::sql {

class ColumnVector;

/// An iterator over the in-memory contents of a column's data.
class ColumnVectorIterator {
 public:
  explicit ColumnVectorIterator(const Schema::ColumnInfo &col_info) noexcept;

  /// Advance this iterator to the next vector of input in the column
  /// \return True if there is more data in the iterator; false otherwise
  bool Advance();

  /// The number of tuples in the input. Or, in other words, the number of
  /// elements in the array returned by \p col_data() or \p col_null_bitmap()
  /// \return The number of tuples in the currently active vector
  u32 NumTuples() const { return next_block_pos() - current_block_pos(); }

  /// Reset the iterator to begin iteration at the start \p column
  /// \param column The column to begin iteration over
  void Reset(const ColumnVector *column);

  /// Access the current vector of column data
  byte *col_data() { return col_data_; }

  /// Access the current NULL bitmap
  u32 *col_null_bitmap() { return col_null_bitmap_; }

 private:
  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  const Schema::ColumnInfo &col_info() const { return col_info_; }

  const ColumnVector *column() const { return column_; }

  i32 current_block_pos() const { return curr_block_pos_; }

  u32 next_block_pos() const { return next_block_pos_; }

 private:
  // The schema information for the column this iterator operates on
  const Schema::ColumnInfo &col_info_;

  // The current block we're iterating over
  const ColumnVector *column_;

  // The current position in the current block
  u32 curr_block_pos_;

  // The position in the current block to find the next vector of input
  u32 next_block_pos_;

  // Pointer to column data
  byte *col_data_;

  // Pointer to the column's bitmap
  u32 *col_null_bitmap_;
};

}  // namespace tpl::sql