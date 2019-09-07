#pragma once

#include <iosfwd>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/common.h"
#include "common/macros.h"
#include "sql/schema.h"
#include "sql/vector.h"

namespace tpl::sql {

class ColumnVectorIterator;

/**
 * A vector projection is a container representing a collection of tuples whose
 * attributes are stored in columnar/vector format. It is used in the execution
 * engine to represent subsets of materialized state including base tables and
 * any intermediate state such as hash tables or sorter instances.
 *
 * Vectors in the projection have a well-defined order and are accessed using
 * this unchanging order. All vectors have the same size.
 *
 * In addition to holding just the vector data, vector projections also contain
 * a selection index vector containing the indexes of the tuples that are
 * externally visible. All column vectors contain references to the selection
 * index vector owned by this projection.
 */
class VectorProjection {
  friend class VectorProjectionIterator;

  // Constant marking an invalid index in the selection vector
  static constexpr sel_t kInvalidPos = std::numeric_limits<sel_t>::max();

 public:
  /**
   * Create an empty and uninitialized vector projection. Users must call
   * @em Initialize() or @em InitializeEmpty() to appropriately initialize the
   * projection with the correct columns.
   *
   * @see Initialize()
   * @see InitializeEmpty()
   */
  VectorProjection();

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(VectorProjection);

  /**
   * Initialize a vector projection and create a vector of the specified type
   * for each type provided in the column metadata list @em column_info. All
   * vectors will reference data owned by this vector projection.
   * @param column_info Metadata for columns in the projection.
   */
  void Initialize(const std::vector<const Schema::ColumnInfo *> &column_info);

  /**
   * Initialize an empty vector projection. This will create an empty vector of
   * the specified type for each type provided in the column metadata list
   * @em column_info. All vectors are referencing vectors that reference data
   * stored (and owned) externally. Column vector data is reset/refreshed
   * through calls to ResetColumn().
   * @param column_info Metadata for columns in the projection.
   */
  void InitializeEmpty(const std::vector<const Schema::ColumnInfo *> &column_info);

  /**
   * Has this projection been filtered through a selection vector?
   * @return True if filtered; false otherwise.
   */
  bool IsFiltered() const { return sel_vector_[0] != kInvalidPos; }

  /**
   * Return a reference to the selection vector. If no selection vector exists
   * for the projection, a null pointer is returned;
   */
  sel_t *GetSelectionVector() { return IsFiltered() ? sel_vector_ : nullptr; }

  /**
   * Set the selection vector for this projection to the contents of the one
   * provided.
   * @param new_sel_vector The new selection vector.
   * @param count The number of elements in the new selection vector.
   */
  void SetSelectionVector(const sel_t *new_sel_vector, uint32_t count);

  /**
   * Access metadata for the column at position @em col_idx in the projection.
   * @return The metadata for the column at the given index in the projection.
   */
  const Schema::ColumnInfo *GetColumnInfo(const uint32_t col_idx) const {
    TPL_ASSERT(col_idx < GetNumColumns(), "Out-of-bounds column access");
    return column_info_[col_idx];
  }

  /**
   * Access the column at index @em col_idx as it appears in the projection.
   * @param col_idx The index of the column.
   * @return The column's vector data.
   */
  const Vector *GetColumn(const uint32_t col_idx) const {
    TPL_ASSERT(col_idx < GetNumColumns(), "Out-of-bounds column access");
    return columns_[col_idx].get();
  }

  /**
   * Access the column at index @em col_idx as it appears in this projection.
   * @param col_idx The index of the column.
   * @return The column's vector data.
   */
  Vector *GetColumn(const uint32_t col_idx) {
    TPL_ASSERT(col_idx < GetNumColumns(), "Out-of-bounds column access");
    return columns_[col_idx].get();
  }

  /**
   * Reset this vector projection to the state after initialization.
   */
  void Reset();

  /**
   * Reset/reload the data for the column at position @em col_idx in the
   * projection using the data from the column iterator at the same position in
   * the provided vector @em column_iterators.
   * @param column_iterators A vector of all column iterators.
   * @param col_idx The index of the column in this projection to reset.
   */
  void ResetColumn(const std::vector<ColumnVectorIterator> &column_iterators, uint32_t col_idx);

  /**
   * Reset/reload the data for the column at position @em col_idx in this
   * projection using @em col_data and @em col_null_bitmap for the raw data and
   * NULL bitmap, respectively.
   * @param col_data The raw (potentially compressed) data for the column.
   * @param col_null_bitmap The null bitmap for the column.
   * @param col_idx The index of the column to reset.
   * @param num_tuples The number of tuples stored in the input.
   */
  void ResetColumn(byte *col_data, uint32_t *col_null_bitmap, uint32_t col_idx,
                   uint32_t num_tuples);

  /**
   * Return the number of columns in this projection.
   */
  uint32_t GetNumColumns() const { return columns_.size(); }

  /**
   * Return the number of active tuples in this projection.
   */
  uint64_t GetTupleCount() const { return columns_.empty() ? 0 : columns_[0]->count(); }

  /**
   * Set the current count of tuples in this projection..
   * @param count The count.
   */
  void SetTupleCount(uint64_t count);

  /**
   * Compute the selectivity of this projection.
   * @return A number between [0.0, 1.0] representing the selectivity, i.e., the
   *         fraction of tuples that are active and visible.
   */
  double ComputeSelectivity() const {
    return columns_.empty() ? 0 : columns_[0]->ComputeSelectivity();
  }

  /**
   * Return a string representation of this vector.
   * @return A string representation of the projection's contents.
   */
  std::string ToString() const;

  /**
   * Print a string representation of this vector projection to the provided
   * output stream @em stream.
   * @param stream The stream where the string representation of this projection
   *               is written to.
   */
  void Dump(std::ostream &stream) const;

  /**
   * Perform an integrity check on this vector projection instance. This is used
   * in debug mode for sanity checks.
   */
  void CheckIntegrity() const;

 private:
  // Metadata for all columns in this projection.
  std::vector<const Schema::ColumnInfo *> column_info_;

  // Vector containing column data for all columns in this projection.
  std::vector<std::unique_ptr<Vector>> columns_;

  // The selection vector for the projection.
  sel_t sel_vector_[kDefaultVectorSize];

  // If the vector projection allocates memory for all contained vectors, this
  // pointer owns that memory.
  std::unique_ptr<byte[]> owned_buffer_;
};

}  // namespace tpl::sql
