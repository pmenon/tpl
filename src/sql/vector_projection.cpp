#include "sql/vector_projection.h"

#include <memory>
#include <vector>

#include "sql/column_vector_iterator.h"
#include "util/bit_util.h"

namespace tpl::sql {

VectorProjection::VectorProjection() : tuple_count_(0), vector_size_(0) {}

VectorProjection::VectorProjection(
    std::vector<const Schema::ColumnInfo *> &col_info, u32 size) {
  Setup(col_info, size);
}

void VectorProjection::Setup(
    std::vector<const Schema::ColumnInfo *> &column_infos, u32 size) {
  const auto num_cols = column_infos.size();
  column_info_ = std::make_unique<const Schema::ColumnInfo *[]>(num_cols);
  column_data_ = std::make_unique<byte *[]>(num_cols);
  column_null_bitmaps_ = std::make_unique<u32 *[]>(num_cols);
  deletions_ = util::BitVector(size);
  tuple_count_ = 0;
  vector_size_ = size;
}

void VectorProjection::ResetColumn(
    const std::vector<ColumnVectorIterator> &col_iters, const u32 col_idx) {
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
                                    const u32 col_idx, const u32 num_tuples) {
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
