#include "sql/generic_hash_table.h"

#include "util/math_util.h"

namespace tpl::sql {

GenericHashTable::GenericHashTable(float load_factor) noexcept
    : entries_(nullptr), mask_(0), capacity_(0), num_elems_(0), load_factor_(load_factor) {}

GenericHashTable::~GenericHashTable() {
  if (entries_ != nullptr) {
    Memory::FreeHugeArray(entries_, GetCapacity());
  }
}

void GenericHashTable::SetSize(uint64_t new_size) {
  TPL_ASSERT(new_size > 0, "New size cannot be zero!");
  if (entries_ != nullptr) {
    Memory::FreeHugeArray(entries_, GetCapacity());
  }

  uint64_t next_size = util::MathUtil::PowerOf2Ceil(new_size);
  if (next_size < new_size / load_factor_) {
    next_size *= 2;
  }

  capacity_ = next_size;
  mask_ = capacity_ - 1;
  num_elems_ = 0;
  entries_ = Memory::MallocHugeArray<std::atomic<HashTableEntry *>>(capacity_, true);
}

// ---------------------------------------------------------
// Vector Iterator
// ---------------------------------------------------------

template <bool UseTag>
GenericHashTableVectorIterator<UseTag>::GenericHashTableVectorIterator(
    const GenericHashTable &table, MemoryPool *memory) noexcept
    : memory_(memory),
      table_(table),
      table_dir_index_(0),
      entry_vec_(
          memory_->AllocateArray<const HashTableEntry *>(kDefaultVectorSize, CACHELINE_SIZE, true)),
      entry_vec_end_idx_(0) {
  Next();
}

template <bool UseTag>
GenericHashTableVectorIterator<UseTag>::~GenericHashTableVectorIterator() {
  memory_->DeallocateArray(entry_vec_, kDefaultVectorSize);
}

template <bool UseTag>
void GenericHashTableVectorIterator<UseTag>::Next() {
  // Invariant: the range of elements [0, entry_vec_end_idx_) in
  // the entry cache contains non-null hash table entries.

  // Index tracks the end of the valid range of entries in the entry cache
  uint32_t index = 0;

  // For the current set of valid entries, follow their chain. This may produce
  // holes in the range, but we'll compact them out in a subsequent filter.
  for (uint32_t i = 0; i < entry_vec_end_idx_; i++) {
    entry_vec_[i] = entry_vec_[i]->next;
  }

  // Compact out the holes produced in the previous chain lookup.
  for (uint32_t i = 0; i < entry_vec_end_idx_; i++) {
    entry_vec_[index] = entry_vec_[i];
    index += (entry_vec_[index] != nullptr);
  }

  // Fill the range [idx, SIZE) in the cache with valid entries from the source
  // hash table.
  while (index < kDefaultVectorSize && table_dir_index_ < table_.GetCapacity()) {
    entry_vec_[index] = table_.entries_[table_dir_index_++].load(std::memory_order_relaxed);
    if constexpr (UseTag) {
      entry_vec_[index] = GenericHashTable::UntagPointer(entry_vec_[index]);
    }
    index += (entry_vec_[index] != nullptr);
  }

  // The new range of valid entries is in [0, idx).
  entry_vec_end_idx_ = index;
}

template class GenericHashTableVectorIterator<true>;
template class GenericHashTableVectorIterator<false>;

}  // namespace tpl::sql
