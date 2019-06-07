#include "sql/aggregation_hash_table.h"

#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

AHTVectorIterator::AHTVectorIterator(
    const AggregationHashTable &agg_hash_table,
    const std::vector<const Schema::ColumnInfo *> &col_infos,
    const AHTVectorIterator::TransposeFn transpose_fn)
    : memory_(agg_hash_table.memory_),
      iter_(agg_hash_table.hash_table_, memory_),
      vector_projection_(
          std::make_unique<VectorProjection>(col_infos, kDefaultVectorSize)),
      vector_projection_iterator_(std::make_unique<VectorProjectionIterator>()),
      temp_aggregates_vec_(memory_->AllocateArray<const byte *>(
          kDefaultVectorSize, CACHELINE_SIZE, false)) {
  //
  // Allocate an array for each aggregate that's been accumulated. This is where
  // the transposed aggregate data in the vector projection will live.
  //

  const auto num_elems = kDefaultVectorSize;
  for (const auto *col_info : col_infos) {
    auto size = col_info->StorageSize() * num_elems;
    auto null_bitmap_size =
        sizeof(u32) * util::BitUtil::Num32BitWordsFor(num_elems);
    auto *data = static_cast<byte *>(memory_->Allocate(size, true));
    auto *nulls = static_cast<u32 *>(memory_->Allocate(null_bitmap_size, true));
    projection_data_.emplace_back(std::make_pair(data, nulls));
  }

  //
  // The hash table iterator may have some data when we instantiated it. Now
  // that the projection is setup, let's build it over the current batch of
  // aggregate input.
  //

  BuildVectorProjection(transpose_fn);
}

AHTVectorIterator::AHTVectorIterator(
    const AggregationHashTable &aht, const Schema::ColumnInfo *col_infos,
    const u32 num_cols, const AHTVectorIterator::TransposeFn transpose_fn)
    : AHTVectorIterator(aht, {col_infos, col_infos + num_cols}, transpose_fn) {}

AHTVectorIterator::~AHTVectorIterator() {
  //
  // Deallocate vector data
  //

  for (u32 i = 0; i < projection_data_.size(); i++) {
    const auto col_info = vector_projection_->GetColumnInfo(i);
    auto size = col_info->StorageSize() * kDefaultVectorSize;
    auto null_bitmap_size =
        sizeof(u32) * util::BitUtil::Num32BitWordsFor(kDefaultVectorSize);
    memory_->Deallocate(projection_data_[i].first, size);
    memory_->Deallocate(projection_data_[i].second, null_bitmap_size);
  }

  //
  // Release the temporary aggregate vector
  //

  memory_->DeallocateArray(temp_aggregates_vec_, kDefaultVectorSize);
}

void AHTVectorIterator::BuildVectorProjection(
    const AHTVectorIterator::TransposeFn transpose_fn) {
  //
  // We have an input batch of hash table entries. For each such entry, we store
  // a pointer to the payload so that the transposition function is unaware.
  //

  auto [size, entries] = iter_.GetCurrentBatch();

  for (u32 i = 0; i < size; i++) {
    temp_aggregates_vec_[i] = entries[i]->payload;
  }

  //
  // We update the vector projection because we may potentially have a new
  // vector size..
  //

  u32 idx = 0;
  for (auto &[col_data, col_null_bitmap] : projection_data_) {
    vector_projection_->ResetFromRaw(col_data, col_null_bitmap, idx++, size);
  }

  vector_projection_iterator_->SetVectorProjection(vector_projection_.get());

  //
  // Setup is done. Now we can invoke the transposition function so let it do
  // the heavy lifting.
  //

  transpose_fn(temp_aggregates_vec_, size, vector_projection_iterator_.get());

  //
  // The vector projection is now filled with aggregate data. Reset the VPI
  // so that it's ready for iteration.
  //

  TPL_ASSERT(!vector_projection_iterator_->IsFiltered(),
             "VPI shouldn't be filtered during a transpose");
  vector_projection_iterator_->Reset();
}

}  // namespace tpl::sql
