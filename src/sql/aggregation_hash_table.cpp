#include "sql/aggregation_hash_table.h"

#include "util/cpu_info.h"

namespace tpl::sql {

AggregationHashTable::AggregationHashTable(util::Region *region,
                                           u32 payload_size) noexcept
    : entries_(region, payload_size),
      max_fill_(static_cast<u64>(kDefaultInitialTableSize * 0.7)) {
  hash_table_.SetSize(kDefaultInitialTableSize);
}

void AggregationHashTable::Grow() {
  // Resize table
  auto new_size = hash_table_.capacity() * 2;
  max_fill_ = static_cast<u64>(new_size * 0.7);
  hash_table_.SetSize(new_size);

  // Insert elements again
  for (const auto &untyped_entry : entries_) {
    auto *entry = reinterpret_cast<HashTableEntry *>(untyped_entry);
    hash_table_.Insert<false>(entry, entry->hash);
  }
}

byte *AggregationHashTable::Insert(hash_t hash) noexcept {
  // Grow if need be
  if (NeedsToGrow()) {
    Grow();
  }

  // Allocate an entry
  auto *entry = reinterpret_cast<HashTableEntry *>(entries_.append());
  entry->hash = hash;
  entry->next = nullptr;

  // Insert into table
  hash_table_.Insert<false>(entry, entry->hash);

  // Give the payload so the client can write into it
  return entry->payload;
}

byte *AggregationHashTable::Lookup(const hash_t hash,
                                   AggregationHashTable::KeyEqFn key_eq_fn,
                                   const void *arg) noexcept {
  auto *entry = hash_table_.FindChainHead(hash);

  while (entry != nullptr) {
    if (entry->hash == hash && key_eq_fn(arg, entry->payload)) {
      return entry->payload;
    }
    entry = entry->next;
  }

  return nullptr;
}

void AggregationHashTable::ProcessBatch(
    VectorProjectionIterator *iters[], AggregationHashTable::HashFn hash_fn,
    AggregationHashTable::InitAggFn init_agg_fn,
    AggregationHashTable::MergeAggFn merge_agg_fn) {
  hash_t hashes[kDefaultVectorSize];
  HashTableEntry *entries[kDefaultVectorSize];

  // First perform the hash lookup
  u64 l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (hash_table_.GetTotalMemoryUsage() > l3_cache_size) {
    ComputeHashAndLookup<true>(iters, hashes, entries, hash_fn);
  } else {
    ComputeHashAndLookup<false>(iters, hashes, entries, hash_fn);
  }

  // Process as appropriate
}

template <bool Prefetch>
void AggregationHashTable::ComputeHashAndLookup(
    VectorProjectionIterator *iters[], hash_t hashes[],
    HashTableEntry *entries[], AggregationHashTable::HashFn hash_fn) {}

}  // namespace tpl::sql
