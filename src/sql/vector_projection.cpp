#include "sql/vector_projection.h"

#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "sql/column_vector_iterator.h"
#include "sql/vector.h"
#include "util/bit_util.h"

namespace tpl::sql {

VectorProjection::VectorProjection() : sel_vector_{0}, owned_buffer_(nullptr) {
  sel_vector_[0] = kInvalidPos;
}

void VectorProjection::InitializeEmpty(
    const std::vector<const Schema::ColumnInfo *> &column_info) {
  TPL_ASSERT(!column_info.empty(),
             "Cannot create projection with zero columns");
  sel_vector_[0] = kInvalidPos;
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

  // Determine the total size of the data chunk we need to support the columns
  // we manage.
  const auto size_in_bytes = std::accumulate(
      columns_.begin(), columns_.end(), 0u, [&](u32 curr_size, auto &col) {
        return curr_size + (GetTypeIdSize(col->type_id()) * kDefaultVectorSize);
      });
  TPL_ASSERT(size_in_bytes > 0, "Cannot have zero-size vector projection");
  owned_buffer_ = std::make_unique<byte[]>(size_in_bytes);

  // Setup the vector's to reference our data chunk
  auto ptr = owned_buffer_.get();
  for (const auto &col : columns_) {
    col->Reference(col->type_id(), ptr, nullptr, 0);
    ptr += GetTypeIdSize(col->type_id()) * kDefaultVectorSize;
  }
}

void VectorProjection::SetSelectionVector(const sel_t *const new_sel_vector,
                                          const u32 count) {
  TPL_ASSERT(new_sel_vector != nullptr, "Null input selection vector");
  TPL_ASSERT(count <= kDefaultVectorSize, "Invalid count");

  // Copy into our selection vector
  std::memcpy(sel_vector_, new_sel_vector, count * sizeof(sel_t));

  // Update vectors
  for (const auto &col : columns_) {
    col->SetSelectionVector(sel_vector_, count);
  }
}

void VectorProjection::Reset() {
  // Reset selection vector.
  sel_vector_[0] = kInvalidPos;

  // Force vector's to reference memory managed by us.
  auto ptr = owned_buffer_.get();
  for (const auto &col : columns_) {
    const auto col_type = col->type_id();
    col->Reference(col_type, ptr, nullptr, 0);
    ptr += GetTypeIdSize(col_type) * kDefaultVectorSize;
  }
}

void VectorProjection::ResetColumn(byte *col_data, u32 *col_null_bitmap,
                                   u32 col_idx, u32 num_tuples) {
  auto col_type = GetColumnInfo(col_idx)->sql_type.GetPrimitiveTypeId();
  columns_[col_idx]->Reference(col_type, col_data, col_null_bitmap, num_tuples);
}

void VectorProjection::ResetColumn(
    const std::vector<ColumnVectorIterator> &column_iterators,
    const u32 col_idx) {
  ResetColumn(column_iterators[col_idx].col_data(),
              column_iterators[col_idx].col_null_bitmap(), col_idx,
              column_iterators[col_idx].NumTuples());
}

void VectorProjection::SetTupleCount(u64 count) {
  for (auto &col : columns_) {
    col->set_count(count);
  }
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

void VectorProjection::CheckIntegrity() const {
#ifndef NDEBUG
  // Check that all contained vectors have the same size and selection vector
  for (const auto &col : columns_) {
    TPL_ASSERT(!IsFiltered() || sel_vector_ == col->selection_vector(),
               "Vector in projection with different selection vector");
    TPL_ASSERT(GetTupleCount() == col->count(),
               "Vector size does not match rest of projection");
  }
  // Let the vectors do an integrity check
  for (const auto &col : columns_) {
    col->CheckIntegrity();
  }
#endif
}

}  // namespace tpl::sql
