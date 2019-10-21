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
class TupleIdList;

/**
 * A container representing a collection of tuples whose attributes are stored in columnar format.
 * It's used in the execution engine to represent subsets of materialized state such partitions of
 * base tables and intermediate state including hash tables or sorter instances.
 *
 * Columns in the projection have a well-defined order and are accessed using this unchanging order.
 * All columns in the projection have the same size and selection count at any given time.
 *
 * In addition to holding just the vector data, vector projections also contain a selection index
 * vector containing the indexes of the tuples that are externally visible. All column vectors
 * contain references to the selection index vector owned by this projection. At any given time,
 * projections have a selected count (see VectorProjection::GetSelectedTupleCount()) that is <=
 * the total tuple count (see VectorProjection::GetTotalTupleCount()). Users can manually set the
 * selections by calling either VectorProjection::SetSelectionVector() with a physical selection
 * index vector, or VectorProjection::SetSelections() providing a tuple ID list.
 *
 * VectorProjections come in two flavors: referencing and owning projections. A referencing vector
 * projection contains a set of column vectors that reference data stored externally. An owning
 * vector projection allocates and owns a chunk of data that it partitions and assigns to all child
 * vectors. By default, it will allocate enough data for each child to have a capacity determined by
 * the global constant ::tpl::kDefaultVectorSize, usually 2048 elements. After construction, and
 * owning vector projection has a <b>zero</b> size (though it's capacity is
 * ::tpl::kDefaultVectorSize). Thus, users must explicitly set the size through
 * VectorProjection::Resize() before interacting with the projection. Resizing sets up all contained
 * column vectors.
 *
 * To create a referencing vector, use VectorProjection::InitializeEmpty(), and fill each column
 * vector with VectorProjection::ResetColumn():
 * @code
 * VectorProjection vp;
 * vp.InitializeEmpty(...);
 * vp.ResetColumn(0, ...);
 * vp.ResetColumn(1, ...);
 * ...
 * @endcode
 *
 * To create an owning vector, use VectorProjection::Initialize():
 * @code
 * VectorProjection vp;
 * vp.Initialize(...);
 * vp.Resize(10);
 * VectorOps::Fill(vp.GetColumn(0), ...);
 * @endcode
 *
 * To remove any selection vector, call VectorProjection::Reset(). This returns the projection to
 * the a state as immediately following initialization.
 *
 * @code
 * VectorProjection vp;
 * vp.Initialize(...);
 * vp.Resize(10);
 * // vp size and selected is 10
 * vp.Reset();
 * // vp size and selected are 0
 * vp.Resize(20);
 * // vp size and selected is 20
 * @endcode
 */
class VectorProjection {
  friend class VectorProjectionIterator;

  // Constant marking an invalid index in the selection vector
  static constexpr sel_t kInvalidPos = std::numeric_limits<sel_t>::max();

 public:
  /**
   * Create an empty and uninitialized vector projection. Users must call @em Initialize() or
   * @em InitializeEmpty() to appropriately initialize the projection with the correct columns.
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
   * Initialize a vector projection with column vectors of the provided types. This will create one
   * vector for each type provided in the column metadata list @em column_info. All vectors will
   * will be initialized with a maximum capacity of kDefaultVectorSize (e.g., 2048), are empty, and
   * will reference data owned by this vector projection.
   * @param column_info Metadata for columns in the projection.
   */
  void Initialize(const std::vector<const Schema::ColumnInfo *> &column_info);

  /**
   * Initialize an empty vector projection with columns of the provided types. This will create an
   * empty vector of the specified type for each type provided in the column metadata list
   * @em column_info. Column vectors may only reference external data set and refreshed through
   * @em ResetColumn().
   *
   * @see VectorProjection::ResetColumn()
   *
   * @param column_info Metadata for columns in the projection.
   */
  void InitializeEmpty(const std::vector<const Schema::ColumnInfo *> &column_info);

  /**
   * Has this projection been filtered through a selection vector?
   * @return True if filtered; false otherwise.
   */
  bool IsFiltered() const { return sel_vector_[0] != kInvalidPos; }

  /**
   * Return the selection vector. If no selection vector exists, returns NULL.
   */
  sel_t *GetSelectionVector() { return IsFiltered() ? sel_vector_ : nullptr; }

  /**
   * Filter elements from the vector based on the indexes in the provided selection index vector.
   * @param new_sel_vector The new selection vector.
   * @param count The number of elements in the new selection vector.
   */
  void SetSelectionVector(const sel_t *new_sel_vector, uint32_t count);

  /**
   * Filter elements from the projection based on the tuple IDs in the input list @em tid_list.
   * @param tid_list The input TID list of valid tuples.
   */
  void SetSelections(const TupleIdList &tid_list);

  /**
   * Access metadata for the column at position @em col_idx in the projection.
   * @return The metadata for the column at the given index in the projection.
   */
  const Schema::ColumnInfo *GetColumnInfo(const uint32_t col_idx) const {
    TPL_ASSERT(col_idx < GetColumnCount(), "Out-of-bounds column access");
    return column_info_[col_idx];
  }

  /**
   * Access the column at index @em col_idx as it appears in the projection.
   * @param col_idx The index of the column.
   * @return The column's vector data.
   */
  const Vector *GetColumn(const uint32_t col_idx) const {
    TPL_ASSERT(col_idx < GetColumnCount(), "Out-of-bounds column access");
    return columns_[col_idx].get();
  }

  /**
   * Access the column at index @em col_idx as it appears in this projection.
   * @param col_idx The index of the column.
   * @return The column's vector data.
   */
  Vector *GetColumn(const uint32_t col_idx) {
    TPL_ASSERT(col_idx < GetColumnCount(), "Out-of-bounds column access");
    return columns_[col_idx].get();
  }

  /**
   * Set the size of this projection and all child vectors to the given size. This size must be less
   * than the maximum capacity of the vector projection.
   * @param num_tuples The size to set the projection.
   */
  void Resize(uint64_t num_tuples);

  /**
   * Reset this vector projection to the state after initialization. This will set the counts to 0,
   * and reset each child vector to point to data owned by this projection (if it owns any).
   */
  void Reset();

  /**
   * Reset the data for the column vector at position @em col_idx in the projection using the data
   * from the column iterator at the same index.
   * @param column_iterators A vector of all column iterators.
   * @param col_idx The index of the column in this projection to reset.
   */
  void ResetColumn(const std::vector<ColumnVectorIterator> &column_iterators, uint32_t col_idx);

  /**
   * Reset/reload the data for the column at position @em col_idx in this projection using
   * @em col_data and @em col_null_bitmap for the raw data and NULL bitmap, respectively.
   * @param col_data The raw (potentially compressed) data for the column.
   * @param col_null_bitmap The null bitmap for the column.
   * @param col_idx The index of the column to reset.
   * @param num_tuples The number of tuples stored in the input.
   */
  void ResetColumn(byte *col_data, uint32_t *col_null_bitmap, uint32_t col_idx,
                   uint32_t num_tuples);

  /**
   * @return The number of columns in the projection.
   */
  uint32_t GetColumnCount() const { return columns_.size(); }

  /**
   * @return The number of active, i.e., externally visible, tuples in this projection. The selected
   *         tuple count is always <= the total tuple count.
   */
  uint64_t GetSelectedTupleCount() const { return columns_.empty() ? 0 : columns_[0]->GetCount(); }

  /**
   * @return The total number of tuples in the projection, including those that may have been
   *         filtered out by a selection vector, if one exists. The total tuple count is >= the
   *         selected tuple count.
   */
  uint64_t GetTotalTupleCount() const { return columns_.empty() ? 0 : columns_[0]->GetSize(); }

  /**
   * @return The maximum capacity of this projection. In other words, the maximum number of tuples
   *         the vectors constituting this projection can store.
   */
  uint64_t GetTupleCapacity() const { return columns_.empty() ? 0 : columns_[0]->GetCapacity(); }

  /**
   * @return The selectivity of this projection, i.e., the fraction of **total** tuples that have
   *         passed any filters (through the selection vector), and are externally visible. The
   *         selectivity is a floating-point number in the range [0.0, 1.0].
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
   * Print a string representation of this vector projection to the provided output stream.
   * @param stream The stream where the string representation of this projection is written to.
   */
  void Dump(std::ostream &stream) const;

  /**
   * Perform an integrity check on this vector projection instance. This is used in debug mode.
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
