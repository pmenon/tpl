#include "sql/sorter.h"

#include <algorithm>
#include <memory>
#include <vector>

#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

SorterVectorIterator::SorterVectorIterator(
    const Sorter &sorter, const std::vector<const Schema::ColumnInfo *> &column_info,
    const SorterVectorIterator::TransposeFn transpose_fn)
    : memory_(sorter.memory_),
      iter_(sorter),
      temp_rows_(memory_->AllocateArray<const byte *>(kDefaultVectorSize, false)),
      vector_projection_(std::make_unique<VectorProjection>()),
      vector_projection_iterator_(std::make_unique<VectorProjectionIterator>()) {
  // First, initialize the vector projection
  vector_projection_->Initialize(column_info);

  // Now, move the iterator to the next valid position
  Next(transpose_fn);
}

SorterVectorIterator::SorterVectorIterator(const Sorter &sorter,
                                           const Schema::ColumnInfo *column_info, uint32_t num_cols,
                                           const SorterVectorIterator::TransposeFn transpose_fn)
    : SorterVectorIterator(sorter, {column_info, column_info + num_cols}, transpose_fn) {}

SorterVectorIterator::~SorterVectorIterator() {
  memory_->DeallocateArray(temp_rows_, kDefaultVectorSize);
}

bool SorterVectorIterator::HasNext() const {
  return vector_projection_->GetSelectedTupleCount() > 0;
}

void SorterVectorIterator::Next(const SorterVectorIterator::TransposeFn transpose_fn) {
  // Pull rows into temporary array
  uint32_t size = std::min(iter_.NumRemaining(), static_cast<uint64_t>(kDefaultVectorSize));
  for (uint32_t i = 0; i < size; ++i, ++iter_) {
    temp_rows_[i] = iter_.GetRow();
  }

  // Setup vector projection
  vector_projection_->Resize(size);

  // Build the vector projection
  if (size > 0) {
    BuildVectorProjection(transpose_fn);
  }
}

void SorterVectorIterator::BuildVectorProjection(
    const SorterVectorIterator::TransposeFn transpose_fn) {
  // Update the vector projection iterator
  vector_projection_iterator_->SetVectorProjection(vector_projection_.get());

  // Invoke the transposition function which does the heavy, query-specific,
  // lifting of converting rows to columns.
  transpose_fn(temp_rows_, vector_projection_->GetSelectedTupleCount(),
               vector_projection_iterator_.get());

  // The vector projection is now filled with sorted rows in columnar format.
  // Reset the VPI so that it's ready for iteration.
  TPL_ASSERT(!vector_projection_iterator_->IsFiltered(),
             "VPI shouldn't be filtered during a transpose");
  vector_projection_iterator_->Reset();

  // Sanity check
  vector_projection_->CheckIntegrity();
}

}  // namespace tpl::sql
