#pragma once

#include "sql/schema.h"

namespace tpl::sql {

class ColumnSegment;

/**
 * A vector-at-a-time iterator over the in-memory contents of a column's data. Each iteration
 * returns at most <i>kDefaultVectorSize</i> (i.e., 2048) elements. After construction, callers
 * initialize the iterator through ColumnVectorIterator::Reset() providing a column segment to
 * iterate over.
 *
 * Use as follows:
 * @code
 * ColumnVectorIterator iter(...);
 * iter.Reset(column_segment);
 * while (iter.Advance()) {
 *   auto *vector_data = iter.col_data();
 *   ...
 * }
 * @endcode
 */
class ColumnVectorIterator {
 public:
  explicit ColumnVectorIterator(const Schema::ColumnInfo *col_info) noexcept;

  /**
   * Advance this iterator to the next vector of input in the column.
   * @return True if there is more data in the iterator; false otherwise.
   */
  bool Advance() noexcept;

  /**
   * Return the number of tuples in the current vector of input data.
   * @return The number of tuples in the currently active vector.
   */
  uint32_t NumTuples() const { return next_block_pos_ - current_block_pos_; }

  /**
   * Reset the iterator to begin iteration at the start of column @em column.
   * @param column The column to begin iteration over.
   */
  void Reset(const ColumnSegment *column) noexcept;

  /**
   * Access the current vector of raw untyped column data.
   */
  byte *col_data() noexcept { return col_data_; }
  byte *col_data() const noexcept { return col_data_; }

  /**
   * Access the current raw NULL bitmap.
   */
  uint32_t *col_null_bitmap() noexcept { return col_null_bitmap_; }
  uint32_t *col_null_bitmap() const noexcept { return col_null_bitmap_; }

 private:
  // The schema information for the column this iterator operates on
  const Schema::ColumnInfo *col_info_;

  // The segment we're currently iterating over
  const ColumnSegment *column_;

  // The current position in the current block
  uint32_t current_block_pos_;

  // The position in the current block to find the next vector of input
  uint32_t next_block_pos_;

  // Pointer to column data
  byte *col_data_;

  // Pointer to the column's bitmap
  uint32_t *col_null_bitmap_;
};

}  // namespace tpl::sql
