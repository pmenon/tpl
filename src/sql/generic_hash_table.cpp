#include "sql/generic_hash_table.h"

#include "util/math_util.h"
#include "util/vector_util.h"

namespace tpl::sql {

GenericHashTable::GenericHashTable(float load_factor) noexcept
    : entries_(nullptr),
      mask_(0),
      capacity_(0),
      num_elems_(0),
      load_factor_(load_factor) {}

GenericHashTable::~GenericHashTable() {
  if (entries_ != nullptr) {
    util::FreeHugeArray(entries_, capacity());
  }
}

void GenericHashTable::SetSize(u64 new_size) {
  TPL_ASSERT(new_size > 0, "New size cannot be zero!");
  if (entries_ != nullptr) {
    util::FreeHugeArray(entries_, capacity());
  }

  u64 next_size = util::MathUtil::PowerOf2Ceil(new_size);
  if (next_size < new_size / load_factor_) {
    next_size *= 2;
  }

  capacity_ = next_size;
  mask_ = capacity_ - 1;
  num_elems_ = 0;
  entries_ = util::MallocHugeArray<std::atomic<HashTableEntry *>>(capacity_);
}

// ---------------------------------------------------------
// Vector Iterator
// ---------------------------------------------------------

template <bool UseTag>
inline void GenericHashTableVectorIterator<UseTag>::Refill() {
  // Invariant: the range of elements [entry_vec_idx_, entry_vec_end_idx_) in
  // the entry cache contains non-null hash table entries.
  //
  // To refill, we first move along the chain of all valid entries in the cache.
  // This may produce some null holes in [entry_vec_idx_, entry_vec_end_idx_).
  // We find all holes and try to fill in entries from the source table. We then
  // left-compact all entries again to ensure the invariant is maintained.

  for (u32 i = 0; i < entry_vec_end_idx_; i++) {
    entry_vec_[i] = entry_vec_[i]->next;
  }

  entry_vec_idx_ = entry_vec_end_idx_ = 0;

  // Try to refill empty entry slots with entries from the source hash table
  if (table_dir_index_ < table_.capacity()) {
    // Find all null slots in the entry vector cache
    const u32 null_count = util::VectorUtil::SelectNull(
        entry_vec_, kDefaultVectorSize, null_slot_sel_vec_, nullptr);

    // For all null-slots, try to plop in an entry from the source table. Fill
    // in slots compactly to the left.
    for (u32 i = 0; i < null_count && table_dir_index_ < table_.capacity();) {
      const auto index = null_slot_sel_vec_[i];
      entry_vec_[index] = table_.entries_[table_dir_index_++];
      if constexpr (UseTag) {
        entry_vec_[index] = GenericHashTable::UntagPointer(entry_vec_[index]);
      }
      i += (entry_vec_[index] != nullptr);
    }
  }

  // Compact the tail
  for (u32 i = entry_vec_end_idx_; i < kDefaultVectorSize; i++) {
    entry_vec_[entry_vec_end_idx_] = entry_vec_[i];
    entry_vec_end_idx_ += (entry_vec_[entry_vec_end_idx_] != nullptr);
  }
  for (u32 i = entry_vec_end_idx_; i < kDefaultVectorSize; i++) {
    entry_vec_[i] = nullptr;
  }
}

template void GenericHashTableVectorIterator<true>::Refill();
template void GenericHashTableVectorIterator<false>::Refill();

}  // namespace tpl::sql
