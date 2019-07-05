#include "sql/vector_projection.h"

#include <memory>
#include <vector>

#include "sql/column_vector_iterator.h"
#include "sql/vector.h"
#include "util/bit_util.h"

namespace tpl::sql {

VectorProjection::VectorProjection() : tuple_count_(0) {}

VectorProjection::VectorProjection(
    const std::vector<const Schema::ColumnInfo *> &col_infos)
    : tuple_count_(0) {
  Setup(col_infos);
}

void VectorProjection::Setup(
    const std::vector<const Schema::ColumnInfo *> &col_infos) {
  // On setup, there's no data so the tuple count is zero
  tuple_count_ = 0;

  // Setup column metadata
  column_info_ = col_infos;

  // Create vectors for each column
  for (const auto &col_info : column_info_) {
    columns_.emplace_back(
        std::make_unique<Vector>(col_info->sql_type.GetPrimitiveTypeId()));
  }
}

void VectorProjection::ResetColumn(
    const std::vector<ColumnVectorIterator> &col_iters, const u32 col_idx) {
  // Read the column's data and NULL bitmap from the iterator
  const auto &col_iter = col_iters[col_idx];
  const auto &col_type = GetColumnInfo(col_idx)->sql_type;
  columns_[col_idx]->Reference(col_type.GetPrimitiveTypeId(),
                               col_iter.col_data(), col_iter.col_null_bitmap(),
                               col_iter.NumTuples());

  // Set the number of active tuples
  tuple_count_ = col_iter.NumTuples();
}

void VectorProjection::ResetFromRaw(byte col_data[], u32 col_null_bitmap[],
                                    const u32 col_idx, const u32 num_tuples) {
  // Reset tuple count
  tuple_count_ = num_tuples;

  // Reset the vector to reference the input data
  auto col_type_id = GetColumnInfo(col_idx)->sql_type.GetPrimitiveTypeId();
  columns_[col_idx]->Reference(col_type_id, col_data, col_null_bitmap,
                               num_tuples);
}

}  // namespace tpl::sql
