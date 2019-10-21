#include "sql/vector_projection.h"

#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "sql/column_vector_iterator.h"
#include "sql/vector.h"

namespace tpl::sql {

VectorProjection::VectorProjection() : sel_vector_{0}, owned_buffer_(nullptr) {
  sel_vector_[0] = kInvalidPos;
}

void VectorProjection::InitializeEmpty(const std::vector<const Schema::ColumnInfo *> &column_info) {
  TPL_ASSERT(!column_info.empty(), "Cannot create projection with zero columns");
  sel_vector_[0] = kInvalidPos;
  column_info_ = column_info;
  columns_.resize(column_info.size());
  for (uint64_t i = 0; i < columns_.size(); i++) {
    const auto col_type = column_info[i]->sql_type.GetPrimitiveTypeId();
    columns_[i] = std::make_unique<Vector>(col_type);
  }
}

void VectorProjection::Initialize(const std::vector<const Schema::ColumnInfo *> &column_info) {
  // First initialize an empty projection
  InitializeEmpty(column_info);

  // Now allocate space to accommodate all child vector data
  std::size_t size_in_bytes = 0;
  for (const auto &col_info : column_info) {
    size_in_bytes += col_info->GetStorageSize() * kDefaultVectorSize;
  }

  // Note that std::make_unique<[]>() will zero-out the array for us due to value-initialization. We
  // don't need to explicitly memset() it.
  owned_buffer_ = std::make_unique<byte[]>(size_in_bytes);

  // Setup the vector's to reference our data chunk
  byte *ptr = owned_buffer_.get();
  for (uint64_t i = 0; i < column_info.size(); i++) {
    columns_[i]->Reference(ptr, nullptr, 0);
    ptr += column_info[i]->GetStorageSize() * kDefaultVectorSize;
  }
}

void VectorProjection::SetSelectionVector(const sel_t *const new_sel_vector, const uint32_t count) {
  TPL_ASSERT(new_sel_vector != nullptr, "Null input selection vector");
  TPL_ASSERT(count <= kDefaultVectorSize, "Invalid count");

  // Copy into our selection vector
  std::memcpy(sel_vector_, new_sel_vector, count * sizeof(sel_t));

  // Update vectors
  for (const auto &col : columns_) {
    col->SetSelectionVector(sel_vector_, count);
  }
}

void VectorProjection::Resize(uint64_t num_tuples) {
  for (auto &col : columns_) {
    col->Resize(num_tuples);
  }
}

void VectorProjection::Reset() {
  // Reset selection vector.
  sel_vector_[0] = kInvalidPos;

  // Force vector's to reference memory managed by us.
  if (owned_buffer_ != nullptr) {
    auto ptr = owned_buffer_.get();
    for (const auto &col : columns_) {
      col->Reference(ptr, nullptr, 0);
      ptr += GetTypeIdSize(col->GetTypeId()) * kDefaultVectorSize;
    }
  }
}

void VectorProjection::ResetColumn(byte *col_data, uint32_t *col_null_bitmap, uint32_t col_idx,
                                   uint32_t num_tuples) {
  columns_[col_idx]->Reference(col_data, col_null_bitmap, num_tuples);
}

void VectorProjection::ResetColumn(const std::vector<ColumnVectorIterator> &column_iterators,
                                   const uint32_t col_idx) {
  ResetColumn(column_iterators[col_idx].GetColumnData(),
              column_iterators[col_idx].GetColumnNullBitmap(), col_idx,
              column_iterators[col_idx].GetTupleCount());
}

std::string VectorProjection::ToString() const {
  std::string result = "VectorProjection(#cols=" + std::to_string(columns_.size()) + "):\n";
  for (auto &col : columns_) {
    result += "- " + col->ToString() + "\n";
  }
  return result;
}

void VectorProjection::Dump(std::ostream &stream) const { stream << ToString() << std::endl; }

void VectorProjection::CheckIntegrity() const {
#ifndef NDEBUG
  // Check that all contained vectors have the same size and selection vector
  for (const auto &col : columns_) {
    TPL_ASSERT(!IsFiltered() || sel_vector_ == col->GetSelectionVector(),
               "Vector in projection with different selection vector");
    TPL_ASSERT(GetSelectedTupleCount() == col->GetCount(),
               "Vector size does not match rest of projection");
  }
  // Let the vectors do an integrity check
  for (const auto &col : columns_) {
    col->CheckIntegrity();
  }
#endif
}

}  // namespace tpl::sql
