#include "sql/generic_hash_table.h"

#include "util/math_util.h"

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
  entries_ = util::MallocHugeArray<std::atomic<HashTableEntry *>>(capacity_);
}

template <bool Prefetch, bool UseTag>
void GenericHashTable::LookupBatch(u32 num_elems, const hash_t hashes[],
                                   HashTableEntry *entries[]) const {
  for (u32 idx = 0, prefetch_idx = kPrefetchDistance; idx < num_elems;
       idx++, prefetch_idx++) {
    // Prefetch if requested
    if constexpr (Prefetch) {
      if (TPL_LIKELY(prefetch_idx < num_elems)) {
        PrefetchChainHead<false>(hashes[prefetch_idx]);
      }
    }

    // Lookup
    if constexpr (UseTag) {
      entries[idx] = FindChainHeadWithTag(hashes[idx]);
    } else {
      entries[idx] = FindChainHead(hashes[idx]);
    }
  }
}

template void GenericHashTable::LookupBatch<false, false>(
    u32, const hash_t[], HashTableEntry *[]) const;
template void GenericHashTable::LookupBatch<false, true>(
    u32, const hash_t[], HashTableEntry *[]) const;
template void GenericHashTable::LookupBatch<true, false>(
    u32, const hash_t[], HashTableEntry *[]) const;
template void GenericHashTable::LookupBatch<true, true>(
    u32, const hash_t[], HashTableEntry *[]) const;

}  // namespace tpl::sql
