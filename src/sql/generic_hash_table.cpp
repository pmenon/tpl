#include "sql/generic_hash_table.h"

#include "util/cpu_info.h"
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
  entries_ = util::MallocHugeArray<HashTableEntry *>(capacity_);
}

void GenericHashTable::LookupChainHeadBatchScalar(
    const u32 num_elems, const hash_t hashes[],
    HashTableEntry *results[]) const {
  for (u32 idx = 0, prefetch_idx = kPrefetchDistance; idx < num_elems; idx++) {
    if (TPL_LIKELY(prefetch_idx < num_elems)) {
      PrefetchChainHead<true>(hashes[prefetch_idx++]);
    }
    results[idx] = FindChainHead(hashes[idx]);
  }
}

void GenericHashTable::LookupChainHeadBatchVector(
    u32 num_elems, const hash_t hashes[], HashTableEntry *results[]) const {
  util::VectorUtil::GatherWithHashes(num_elems, entries_, hashes, mask_,
                                     results);
}

void GenericHashTable::LookupChainHeadBatch(const u32 num_elems,
                                            const hash_t hashes[],
                                            HashTableEntry *results[]) const {
  u64 l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (GetTotalMemoryUsage() > l3_cache_size) {
    LookupChainHeadBatchScalar(num_elems, hashes, results);
  } else {
    LookupChainHeadBatchVector(num_elems, hashes, results);
  }
}

// ---------------------------------------------------------
// Vector Iterator
// ---------------------------------------------------------

template <bool UseTag>
inline void GenericHashTableVectorIterator<UseTag>::Next() {
  // Invariant: the range of elements [0, entry_vec_end_idx_) in
  // the entry cache contains non-null hash table entries.

  // Index tracks the end of the valid range of entries in the entry cache
  u32 index = 0;

  // For the current set of valid entries, follow their chain. This may produce
  // holes in the range, but we'll compact them out in a subsequent filter.
  for (u32 i = 0; i < entry_vec_end_idx_; i++) {
    entry_vec_[i] = entry_vec_[i]->next;
  }

  // Compact out the holes produced in the previous chain lookup.
  for (u32 i = 0; i < entry_vec_end_idx_; i++) {
    entry_vec_[index] = entry_vec_[i];
    index += (entry_vec_[index] != nullptr);
  }

  // Fill the range [idx, SIZE) in the cache with valid entries from the source
  // hash table.
  while (index < kDefaultVectorSize && table_dir_index_ < table_.capacity()) {
    entry_vec_[index] = table_.entries_[table_dir_index_++];
    if constexpr (UseTag) {
      entry_vec_[index] = GenericHashTable::UntagPointer(entry_vec_[index]);
    }
    index += (entry_vec_[index] != nullptr);
  }

  // The new range of valid entries is in [0, idx).
  entry_vec_end_idx_ = index;
}

template void GenericHashTableVectorIterator<true>::Next();
template void GenericHashTableVectorIterator<false>::Next();

}  // namespace tpl::sql
