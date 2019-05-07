#include "sql/aggregation_hash_table.h"

#include "sql/vector_projection_iterator.h"
#include "util/cpu_info.h"
#include "util/vector_util.h"

namespace tpl::sql {

AggregationHashTable::AggregationHashTable(util::Region *region,
                                           u32 payload_size)
    : entries_(region, payload_size),
      max_fill_(std::llround(kDefaultInitialTableSize * kDefaultLoadFactor)) {
  hash_table_.SetSize(kDefaultInitialTableSize);
}

void AggregationHashTable::Grow() {
  // Resize table
  const u64 new_size = hash_table_.capacity() * 2;
  max_fill_ = std::llround(new_size * kDefaultLoadFactor);
  hash_table_.SetSize(new_size);

  // Insert elements again
  for (byte *untyped_entry : entries_) {
    auto *entry = reinterpret_cast<HashTableEntry *>(untyped_entry);
    hash_table_.Insert<false>(entry, entry->hash);
  }
}

HashTableEntry *AggregationHashTable::CreateEntry(const hash_t hash) {
  auto *entry = reinterpret_cast<HashTableEntry *>(entries_.append());
  entry->hash = hash;
  entry->next = nullptr;
  return entry;
}

byte *AggregationHashTable::Insert(const hash_t hash) {
  // Grow if need be
  if (NeedsToGrow()) {
    Grow();
  }

  // Allocate an entry
  HashTableEntry *entry = CreateEntry(hash);

  // Insert into table
  hash_table_.Insert<false>(entry, entry->hash);

  // Give the payload so the client can write into it
  return entry->payload;
}

byte *AggregationHashTable::Lookup(const hash_t hash,
                                   AggregationHashTable::KeyEqFn key_eq_fn,
                                   const void *probe) {
  auto *entry = hash_table_.FindChainHead(hash);

  while (entry != nullptr) {
    if (entry->hash == hash && key_eq_fn(entry->payload, probe)) {
      return entry->payload;
    }
    entry = entry->next;
  }

  return nullptr;
}

void AggregationHashTable::ProcessBatch(
    VectorProjectionIterator *iters[], AggregationHashTable::HashFn hash_fn,
    KeyEqFn key_eq_fn, AggregationHashTable::InitAggFn init_agg_fn,
    AggregationHashTable::MergeAggFn merge_agg_fn) {
  TPL_ASSERT(iters != nullptr, "Null input iterators!");
  const u32 num_elems = iters[0]->num_selected();

  // Temporary vector for the hash values and hash table entry pointers
  hash_t hashes[kDefaultVectorSize];
  HashTableEntry *entries[kDefaultVectorSize];

  u64 l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (hash_table_.GetTotalMemoryUsage() > l3_cache_size) {
    ProcessBatchImpl<true>(iters, num_elems, hashes, entries, hash_fn,
                           key_eq_fn, init_agg_fn, merge_agg_fn);
  } else {
    ProcessBatchImpl<false>(iters, num_elems, hashes, entries, hash_fn,
                            key_eq_fn, init_agg_fn, merge_agg_fn);
  }
}

template <bool Prefetch>
void AggregationHashTable::ProcessBatchImpl(
    VectorProjectionIterator *iters[], u32 num_elems, hash_t hashes[],
    HashTableEntry *entries[], AggregationHashTable::HashFn hash_fn,
    KeyEqFn key_eq_fn, AggregationHashTable::InitAggFn init_agg_fn,
    AggregationHashTable::MergeAggFn merge_agg_fn) {
  // Vector storing all the missing group IDs
  u32 missing_group_vec[kDefaultVectorSize];

  // Lookup batch
  LookupBatch<Prefetch>(iters, num_elems, hashes, entries, hash_fn, key_eq_fn);

  // Determine which elements are missing a group
  u32 num_missing_groups = util::VectorUtil::FilterEq(
      reinterpret_cast<intptr_t *>(entries), iters[0]->num_selected(),
      intptr_t(0), missing_group_vec, nullptr);

  for (u32 idx = 0; idx < num_missing_groups; idx++) {
    hash_t hash = hashes[missing_group_vec[idx]];
    HashTableEntry *entry = CreateEntry(hash);
    init_agg_fn(entry->payload, iters);
    entries[missing_group_vec[idx]] = entry;
  }
}

template <bool Prefetch>
void AggregationHashTable::LookupBatch(
    VectorProjectionIterator *iters[], u32 num_elems, hash_t hashes[],
    HashTableEntry *entries[], AggregationHashTable::HashFn hash_fn,
    AggregationHashTable::KeyEqFn key_eq_fn) const {
  // Compute hash and perform initial lookup
  ComputeHashAndLoadInitial<Prefetch>(iters, num_elems, hashes, entries,
                                      hash_fn);

  // Determine the indexes of entries that are non-null
  u32 group_sel[kDefaultVectorSize];
  u32 num_groups = util::VectorUtil::FilterNe(
      reinterpret_cast<intptr_t *>(entries), iters[0]->num_selected(),
      intptr_t(0), group_sel, nullptr);

  // Candidate groups in 'entries' may have hash collisions. Follow the chain
  // to check key equality.
  if (iters[0]->IsFiltered()) {
    FollowNextLoop<Prefetch, true>(iters, num_groups, group_sel, hashes,
                                   entries, key_eq_fn);
  } else {
    FollowNextLoop<Prefetch, false>(iters, num_groups, group_sel, hashes,
                                    entries, key_eq_fn);
  }
}

template <bool Prefetch>
u32 AggregationHashTable::ComputeHashAndLoadInitial(
    VectorProjectionIterator *iters[], u32 num_elems, hash_t hashes[],
    HashTableEntry *entries[], AggregationHashTable::HashFn hash_fn) const {
  // Compute hash
  if (auto *vpi = iters[0]; vpi->IsFiltered()) {
    for (u32 idx = 0; vpi->HasNextFiltered(); vpi->AdvanceFiltered()) {
      hashes[idx++] = hash_fn(iters);
    }
  } else {
    for (u32 idx = 0; vpi->HasNext(); vpi->Advance()) {
      hashes[idx++] = hash_fn(iters);
    }
  }
  // Reset VPI
  iters[0]->Reset();

  // Load entries
  u32 found = 0;
  for (u32 idx = 0, prefetch_idx = kPrefetchDistance; idx < num_elems;
       idx++, prefetch_idx++) {
    if constexpr (Prefetch) {
      if (TPL_LIKELY(prefetch_idx < num_elems)) {
        hash_table_.PrefetchChainHead<false>(hashes[prefetch_idx]);
      }
    }

    // Follow chain to find first hash match
    HashTableEntry *entry = hash_table_.FindChainHead(hashes[idx]);
    if (entry != nullptr && entry->hash != hashes[idx]) {
      for (; entry != nullptr; entry = entry->next) {
        if (entry->hash == hashes[idx]) {
          found++;
          break;
        }
      }
    }
    entries[idx] = entry;
  }

  return found;
}

template <bool Prefetch, bool VPIIsFiltered>
void AggregationHashTable::FollowNextLoop(
    VectorProjectionIterator *iters[], u32 num_elems, u32 group_sel[],
    const hash_t hashes[], HashTableEntry *entries[],
    AggregationHashTable::KeyEqFn key_eq_fn) const {
  // TODO(pmenon): Use prefetch
  while (num_elems > 0) {
    u32 write_idx = 0;

    // First, check key equality for selected groups
    if constexpr (VPIIsFiltered) {
      for (u32 idx = 0, prev_group_idx = group_sel[0]; idx < num_elems; idx++) {
        // The group we're checking
        const u32 group_idx = group_sel[idx];
        // Advance the iterator to the group position
        iters[0]->AdvanceFiltered(group_idx - prev_group_idx);
        // Check
        const bool not_equal = (entries[group_idx]->hash != hashes[group_idx] ||
                                !key_eq_fn(entries[group_idx]->payload, iters));
        // If the keys didn't match, write index to move forward
        group_sel[write_idx] = group_idx;
        write_idx += static_cast<u32>(not_equal);
        // Move along
        prev_group_idx = group_idx;
      }
    } else {
      for (u32 idx = 0, prev_group_idx = group_sel[0]; idx < num_elems; idx++) {
        // The group we're checking
        const u32 group_idx = group_sel[idx];
        // Advance iterator to the group position
        iters[0]->Advance(group_idx - prev_group_idx);
        // Check
        const bool not_equal = (entries[group_idx]->hash != hashes[group_idx] ||
                                !key_eq_fn(entries[group_idx]->payload, iters));
        // If the keys didn't match, write index to move forward
        group_sel[write_idx] = group_idx;
        write_idx += static_cast<u32>(not_equal);
        // Move along
        prev_group_idx = group_idx;
      }
    }
    // Reset VPI
    iters[0]->Reset();

    // For any unmatched, move forward
    for (u32 idx = 0; idx < write_idx; idx++) {
      HashTableEntry *&entry = entries[group_sel[idx]];
      entry = entry->next;
    }

    // Next
    num_elems = write_idx;
  }
}

}  // namespace tpl::sql
