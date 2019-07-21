#pragma once

#include <iosfwd>
#include <memory>
#include <vector>

#include "sql/schema.h"
#include "sql/vector.h"
#include "util/common.h"
#include "util/macros.h"

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
   * @em Setup() to initialize the projection with the correct columns.
   */
  VectorProjection();

  /**
   * Create a vector projection using the column information provided in
   * @em col_infos.
   * @param col_infos The metadata of each column in the projection.
   */
  explicit VectorProjection(
      const std::vector<const Schema::ColumnInfo *> &col_infos);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(VectorProjection);

  /**
   * Initialize a vector projection and create a vector of the specified type
   * for each type provided in the column metadata list @em col_infos. All
   * vectors will allocate data, but are initially empty.
   * @param col_infos Metadata for columns in the projection.
   */
  void Initialize(const std::vector<const Schema::ColumnInfo *> &col_infos);

  /**
   * Initializes an empty vector projection. This will create an empty vector of
   * the specified type for each type provided in the column metadata list
   * @em col_infos. Vectors will **NOT** allocate data and are only allowed to
   * reference data stored and owned externally through calls to ResetColumn().
   * @param col_infos Metadata for columns in the projection.
   */
  void InitializeEmpty(
      const std::vector<const Schema::ColumnInfo *> &col_infos);

  /**
   * Has this projection been filtered through a selection vector?
   * @return True if filtered; false otherwise.
   */
  bool IsFiltered() const { return sel_vector_[0] != kInvalidPos; }

  /**
   * Access metadata for the column at position @em col_idx in the projection.
   * @return The metadata for the column at the given index in the projection.
   */
  const Schema::ColumnInfo *GetColumnInfo(const u32 col_idx) const {
    TPL_ASSERT(col_idx < GetNumColumns(), "Out-of-bounds column access");
    return column_info_[col_idx];
  }

  /**
   * Access the column at index @em col_idx as it appears in the projection.
   * @param col_idx The index of the column.
   * @return The column's vector data.
   */
  const Vector *GetColumn(const u32 col_idx) const {
    TPL_ASSERT(col_idx < GetNumColumns(), "Out-of-bounds column access");
    return columns_[col_idx].get();
  }

  /**
   * Access the column at index @em col_idx as it appears in this projection.
   * @param col_idx The index of the column.
   * @return The column's vector data.
   */
  Vector *GetColumn(const u32 col_idx) {
    TPL_ASSERT(col_idx < GetNumColumns(), "Out-of-bounds column access");
    return columns_[col_idx].get();
  }

  /**
   * Reset/reload the data for the column at position @em col_idx in the
   * projection using the data from the column iterator at the same position in
   * the provided vector @em col_iters.
   * @param col_iters A vector of all column iterators.
   * @param col_idx The index of the column in this projection to reset.
   */
  void ResetColumn(const std::vector<ColumnVectorIterator> &col_iters,
                   u32 col_idx);

  /**
   * Reset/reload the data for the column at position @em col_idx in this
   * projection using @em col_data and @em col_null_bitmap for the raw data and
   * NULL bitmap, respectively.
   * @param col_data The raw (potentially compressed) data for the column.
   * @param col_null_bitmap The null bitmap for the column.
   * @param col_idx The index of the column to reset.
   * @param num_tuples The number of tuples stored in the input.
   */
  void ResetColumn(byte *col_data, u32 *col_null_bitmap, u32 col_idx,
                   u32 num_tuples);

  /**
   * Return the number of columns in this projection.
   */
  u32 GetNumColumns() const { return columns_.size(); }

  /**
   * Return the number of active tuples in this projection.
   */
  u32 GetTupleCount() const { return tuple_count_; }

  /**
   * Convert to and return a string representation of this vector.
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

 private:
  // Column metadata
  std::vector<const Schema::ColumnInfo *> column_info_;

  // The column's data
  std::vector<std::unique_ptr<Vector>> columns_;

  // The selection vector of the projection
  alignas(CACHELINE_SIZE) sel_t sel_vector_[kDefaultVectorSize];

  // The number of active tuples; either the number of elements in the selection
  // vector if it's in use, or the total number of elements in the projection.
  u32 tuple_count_;
};

}  // namespace tpl::sql
