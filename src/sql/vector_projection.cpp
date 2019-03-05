#include "sql/vector_projection.h"

#include "sql/column_vector_iterator.h"
#include "util/bit_util.h"

namespace tpl::sql {

VectorProjection::VectorProjection(u32 num_cols, u32 size)
    : column_data_(std::make_unique<byte *[]>(num_cols)),
      column_null_bitmaps_(std::make_unique<u32 *[]>(num_cols)),
      deletions_(size),
      tuple_count_(0),
      vector_size_(size) {}

void VectorProjection::ResetColumn(
    const std::vector<ColumnVectorIterator> &col_iters, u32 col_idx) {
  // Read the column's data and NULL bitmap from the iterator
  const ColumnVectorIterator &col_iter = col_iters[col_idx];
  column_data_[col_idx] = col_iter.col_data();
  column_null_bitmaps_[col_idx] = col_iter.col_null_bitmap();

  // Set the number of active tuples
  tuple_count_ = col_iter.NumTuples();

  // Clear the column deletions
  TPL_ASSERT(col_iter.NumTuples() <= vector_size_,
             "Provided column iterator has too many tuples for this vector "
             "projection");
  ClearDeletions();
}

void VectorProjection::ResetFromRaw(byte col_data[], u32 col_null_bitmap[],
                                    u32 col_idx, u32 num_tuples) {
  column_data_[col_idx] = col_data;
  column_null_bitmaps_[col_idx] = col_null_bitmap;
  tuple_count_ = num_tuples;

  TPL_ASSERT(num_tuples <= vector_size_,
             "Provided column iterator has too many tuples for this vector "
             "projection");

  ClearDeletions();
}

void VectorProjection::ClearDeletions() { deletions_.ClearAll(); }

}  // namespace tpl::sql
