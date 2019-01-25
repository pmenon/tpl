#pragma once

#include <memory>
#include <vector>

#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql {

class ColumnVectorIterator;

/// A VectorProjection is a container representing a logical collection of
/// tuples whose projected columns are stored in columnar format
class VectorProjection {
 public:
  VectorProjection(u32 num_cols, u32 size);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(VectorProjection);

  /// Get the current vector input for the column at index \refitem col_idx
  /// \tparam T The data type to interpret the column's data as
  /// \param col_idx The index of the column
  /// \return The typed vector of column data in this vector projection
  template <typename T>
  const T *GetVectorAs(u32 col_idx) const {
    return reinterpret_cast<T *>(column_data_[col_idx]);
  }

  /// Return the NULL bit vector for the column at index \refitem col_idx
  /// \param col_idx The index of the column
  /// \return The NULL bit vector for the desired column
  const u32 *GetNullBitmap(u32 col_idx) const {
    return column_null_bitmaps_[col_idx];
  }

  /// Reset/reload the data for the column at the given index from the given
  /// column iterator instance
  /// \param column_iters A vector of all column iterators
  /// \param col_idx The index of the column in this projection to reset
  void ResetColumn(std::vector<ColumnVectorIterator> &column_iters, u32 col_idx);

  ///
  /// \param column_data
  /// \param column_null_bitmap
  /// \param column_idx
  /// \param num_tuples
  void ResetFromRaw(byte *column_data, u32 *column_null_bitmap,
                    u32 column_idx, u32 num_tuples);

  /// Return the number of active tuples in this projection
  /// \return The number of active tuples
  u32 TotalTupleCount() const { return tuple_count_; }

 private:
  // Set the deletions bitmap
  void ClearDeletions();

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  u32 *deletions() { return deletions_.get(); }

  u32 vector_size() const { return vector_size_; }

 private:
  // The array of pointers to column data for all columns in this projection
  std::unique_ptr<byte *[]> column_data_;

  // The array of pointers to column NULL bitmaps for all columns in this
  // projection
  std::unique_ptr<u32 *[]> column_null_bitmaps_;

  // A bitmap tracking which tuples have been marked for deletion
  std::unique_ptr<u32[]> deletions_;

  // The number of active tuples
  u32 tuple_count_;

  // The maximum supported size of input tuple vectors
  u32 vector_size_;
};

}  // namespace tpl::sql