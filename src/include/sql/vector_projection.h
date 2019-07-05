#pragma once

#include <memory>
#include <vector>

#include "sql/schema.h"
#include "sql/vector.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql {

class ColumnVectorIterator;

/**
 * A VectorProjection is a container representing a logical collection of
 * tuples whose columns are stored in columnar format.
 */
class VectorProjection {
 public:
  /**
   * Create an empty and uninitialized vector projection. Users must call
   * @em Setup() to initialize the projection with the correct columns.
   */
  VectorProjection();

  /**
   * Create a vector projection using the column information provided in
   * @em col_infos.
   */
  explicit VectorProjection(
      const std::vector<const Schema::ColumnInfo *> &col_infos);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(VectorProjection);

  /**
   * Set up this vector projection with the given column information and vector
   * size.
   * @param col_infos Metadata for columns in this projection.
   * @param size The maximum number of elements in the projection.
   */
  void Setup(const std::vector<const Schema::ColumnInfo *> &col_infos);

  /**
   * Access metadata for the column at position @em col_idx in this projection.
   */
  const Schema::ColumnInfo *GetColumnInfo(const u32 col_idx) const {
    TPL_ASSERT(col_idx < num_columns(), "Out-of-bounds column access");
    return column_info_[col_idx];
  }

  /**
   * Access the column at index @em col_idx as it appears in this projection.
   * @param col_idx The index of the column.
   * @return The column's vector.
   */
  const Vector *GetColumn(const u32 col_idx) const {
    TPL_ASSERT(col_idx < num_columns(), "Out-of-bounds column access");
    return columns_[col_idx].get();
  }

  /**
   * Access the column at index @em col_idx as it appears in this projection.
   * @param col_idx The index of the column.
   * @return The column's vector.
   */
  Vector *GetColumn(const u32 col_idx) {
    TPL_ASSERT(col_idx < num_columns(), "Out-of-bounds column access");
    return columns_[col_idx].get();
  }

  /**
   * Reset/reload the data for the column at the given index from the given
   * column iterator instance.
   * @param col_iters A vector of all column iterators.
   * @param col_idx The index of the column in this projection to reset.
   */
  void ResetColumn(const std::vector<ColumnVectorIterator> &col_iters,
                   u32 col_idx);

  /**
   * Reset the column data at index \a col_idx with \a col_data and the.
   * @param col_data The raw (potentially compressed) data for the column.
   * @param col_null_bitmap The null bitmap for the column.
   * @param col_idx The index of the column to reset.
   * @param num_tuples The number of tuples stored in the input.
   */
  void ResetFromRaw(byte col_data[], u32 col_null_bitmap[], u32 col_idx,
                    u32 num_tuples);

  /**
   * Return the number of columns in this projection.
   */
  u32 num_columns() const { return static_cast<u32>(columns_.size()); }

  /**
   * Return the number of active tuples in this projection.
   */
  u32 total_tuple_count() const { return tuple_count_; }

 private:
  // Column meta
  std::vector<const Schema::ColumnInfo *> column_info_;
  // The column's data
  std::vector<std::unique_ptr<Vector>> columns_;
  // The number of active tuples
  u32 tuple_count_;
};

}  // namespace tpl::sql
