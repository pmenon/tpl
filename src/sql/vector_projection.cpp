#include "sql/vector_projection.h"

#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "sql/column_vector_iterator.h"
#include "sql/tuple_id_list.h"
#include "sql/vector.h"

namespace tpl::sql {

VectorProjection::VectorProjection()
    : filter_(nullptr), owned_tid_list_(kDefaultVectorSize), owned_buffer_(nullptr) {
  owned_tid_list_.Resize(0);
}

void VectorProjection::InitializeEmpty(const std::vector<TypeId> &col_types) {
  TPL_ASSERT(!col_types.empty(), "Cannot create projection with zero columns");
  columns_.reserve(col_types.size());
  for (const auto col_type : col_types) {
    columns_.emplace_back(std::make_unique<Vector>(col_type));
  }
}

void VectorProjection::Initialize(const std::vector<TypeId> &col_types) {
  // First initialize an empty projection
  InitializeEmpty(col_types);

  // Now allocate space to accommodate all child vector data
  std::size_t size_in_bytes = 0;
  for (const auto &col_type : col_types) {
    size_in_bytes += GetTypeIdSize(col_type) * kDefaultVectorSize;
  }

  // std::make_unique() with an array-type zeros the array for us due to
  // value-initialization. We don't need to explicitly memset() it.
  owned_buffer_ = std::make_unique<byte[]>(size_in_bytes);

  // Setup the vector's to reference our data chunk
  byte *ptr = owned_buffer_.get();
  for (uint64_t i = 0; i < col_types.size(); i++) {
    columns_[i]->Reference(ptr, nullptr, 0);
    ptr += GetTypeIdSize(col_types[i]) * kDefaultVectorSize;
  }
}

void VectorProjection::RefreshFilteredTupleIdList() {
  // If the list of active TIDs is a strict subset of all TIDs in the projection,
  // we need to update the cached filter list. Otherwise, we set the filter list
  // to NULL to indicate the non-existence of a filter. In either case, we also
  // propagate the list to all child vectors.

  uint32_t count = owned_tid_list_.GetTupleCount();

  if (count < owned_tid_list_.GetCapacity()) {
    filter_ = &owned_tid_list_;
  } else {
    filter_ = nullptr;
    count = owned_tid_list_.GetCapacity();
  }

  for (auto &col : columns_) {
    col->SetFilteredTupleIdList(filter_, count);
  }
}

void VectorProjection::SetFilteredSelections(const TupleIdList &tid_list) {
  TPL_ASSERT(tid_list.GetCapacity() == owned_tid_list_.GetCapacity(),
             "Input TID list capacity doesn't match projection capacity");

  // Copy the input TID list.
  owned_tid_list_.AssignFrom(tid_list);

  // Let the child vectors know of the new list, if need be.
  RefreshFilteredTupleIdList();
}

void VectorProjection::Reset(uint64_t num_tuples) {
  // Reset the cached TID list to NULL indicating all TIDs are active
  filter_ = nullptr;

  // Setup TID list to include all tuples
  owned_tid_list_.Resize(num_tuples);
  owned_tid_list_.AddAll();

  // If the projection is an owning projection, we need to reset each child
  // vector to point to its designated chunk of the internal buffer. If the
  // projection is a referencing projection, just notify each child vector of
  // its new size.

  if (owned_buffer_ != nullptr) {
    auto ptr = owned_buffer_.get();
    for (const auto &col : columns_) {
      col->Reference(ptr, nullptr, num_tuples);
      ptr += GetTypeIdSize(col->GetTypeId()) * kDefaultVectorSize;
    }
  } else {
    for (auto &col : columns_) {
      col->Resize(num_tuples);
    }
  }
}

void VectorProjection::Pack() {
  if (!IsFiltered()) {
    return;
  }

  filter_ = nullptr;
  owned_tid_list_.Resize(GetSelectedTupleCount());
  owned_tid_list_.AddAll();

  for (auto &col : columns_) {
    col->Pack();
  }
}

std::string VectorProjection::ToString() const {
  std::string result = "VectorProjection(#cols=" + std::to_string(columns_.size()) + "):\n";
  for (auto &col : columns_) {
    result += "- " + col->ToString() + "\n";
  }
  return result;
}

void VectorProjection::Dump(std::ostream &os) const { os << ToString() << std::endl; }

void VectorProjection::CheckIntegrity() const {
#ifndef NDEBUG
  // Check that the TID list size is sufficient for this vector projection
  TPL_ASSERT(owned_tid_list_.GetCapacity() == GetTotalTupleCount(),
             "TID list capacity doesn't match vector projection capacity!");

  // Check if the filtered TID list matches the owned list when filtered
  TPL_ASSERT(!IsFiltered() || filter_ == &owned_tid_list_,
             "Filtered list pointer doesn't match internal owned active TID list");

  // Check that all contained vectors have the same size and selection vector
  for (const auto &col : columns_) {
    TPL_ASSERT(!IsFiltered() || filter_ == col->GetFilteredTupleIdList(),
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
