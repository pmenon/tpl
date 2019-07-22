#include "sql/vector_projection.h"

#include <iostream>
#include <memory>
#include <vector>

#include "sql/column_vector_iterator.h"
#include "sql/vector.h"
#include "util/bit_util.h"

namespace tpl::sql {

VectorProjection::VectorProjection()
    : sel_vector_{0}, tuple_count_(0), owned_buffer_(nullptr) {
  sel_vector_[0] = kInvalidPos;
}

void VectorProjection::InitializeEmpty(
    const std::vector<const Schema::ColumnInfo *> &column_info) {
  TPL_ASSERT(!column_info.empty(),
             "Cannot create projection with zero columns");
  sel_vector_[0] = kInvalidPos;
  tuple_count_ = 0;
  column_info_ = column_info;
  columns_.resize(column_info.size());
  for (u32 i = 0; i < columns_.size(); i++) {
    const auto col_type = column_info[i]->sql_type.GetPrimitiveTypeId();
    columns_[i] = std::make_unique<Vector>(col_type);
  }
}

void VectorProjection::Initialize(
    const std::vector<const Schema::ColumnInfo *> &column_info) {
  InitializeEmpty(column_info);

  // Determine total size of projection in bytes
  std::size_t size_in_bytes = 0;
  for (const auto &col : columns_) {
    size_in_bytes += GetTypeIdSize(col->type_id()) * kDefaultVectorSize;
  }
  owned_buffer_ = std::make_unique<byte[]>(size_in_bytes);

  // Update vectors to reference the buffer we created
  byte *ptr = owned_buffer_.get();
  for (const auto &col : columns_) {
    col->Reference(col->type_id(), ptr, nullptr, kDefaultVectorSize);
    ptr += GetTypeIdSize(col->type_id()) * kDefaultVectorSize;
  }
}

void VectorProjection::ResetColumn(byte *col_data, u32 *col_null_bitmap,
                                   u32 col_idx, u32 num_tuples) {
  // Reset tuple count
  tuple_count_ = num_tuples;

  // Reset the vector to reference the input data
  auto col_type_id = GetColumnInfo(col_idx)->sql_type.GetPrimitiveTypeId();
  columns_[col_idx]->Reference(col_type_id, col_data, col_null_bitmap,
                               num_tuples);
}

void VectorProjection::ResetColumn(
    const std::vector<ColumnVectorIterator> &column_iterators,
    const u32 col_idx) {
  ResetColumn(column_iterators[col_idx].col_data(),
              column_iterators[col_idx].col_null_bitmap(), col_idx,
              column_iterators[col_idx].NumTuples());
}

std::string VectorProjection::ToString() const {
  std::string result =
      "VectorProjection(#cols=" + std::to_string(columns_.size()) + "):\n";
  for (auto &col : columns_) {
    result += "- " + col->ToString() + "\n";
  }
  return result;
}

void VectorProjection::Dump(std::ostream &stream) const {
  stream << ToString() << std::endl;
}

}  // namespace tpl::sql
