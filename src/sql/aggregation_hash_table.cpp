#include "sql/aggregation_hash_table.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include <tbb/tbb.h>  // NOLINT

#include "count/hll.h"

#include "logging/logger.h"
#include "sql/thread_state_container.h"
#include "sql/vector_projection_iterator.h"
#include "util/bit_util.h"
#include "util/cpu_info.h"
#include "util/math_util.h"
#include "util/timer.h"
#include "util/vector_util.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Batch Process State
// ---------------------------------------------------------

AggregationHashTable::BatchProcessState::BatchProcessState(
    std::unique_ptr<libcount::HLL> estimator)
    : hll_estimator(std::move(estimator)),
      hashes{0},
      entries{nullptr},
      groups_found{0} {}

AggregationHashTable::BatchProcessState::~BatchProcessState() = default;

// ---------------------------------------------------------
// Aggregation Hash Table
// ---------------------------------------------------------

AggregationHashTable::AggregationHashTable(MemoryPool *memory,
                                           std::size_t payload_size)
    : AggregationHashTable(memory, payload_size, kDefaultInitialTableSize) {}

AggregationHashTable::AggregationHashTable(MemoryPool *memory,
                                           const std::size_t payload_size,
                                           const u32 initial_size)
    : memory_(memory),
      payload_size_(payload_size),
      entries_(sizeof(HashTableEntry) + payload_size_,
               MemoryPoolAllocator<byte>(memory_)),
      owned_entries_(memory_),
      hash_table_(kDefaultLoadFactor),
      batch_state_(nullptr),
      merge_partition_fn_(nullptr),
      partition_heads_(nullptr),
      partition_tails_(nullptr),
      partition_estimates_(nullptr),
      partition_tables_(nullptr),
      partition_shift_bits_(
          util::BitUtil::CountLeadingZeros(u64(kDefaultNumPartitions) - 1)) {
  hash_table_.SetSize(initial_size);
  max_fill_ =
      u64(std::llround(hash_table_.capacity() * hash_table_.load_factor()));

  // Compute flush threshold. In partitioned mode, we want the thread-local
  // pre-aggregation hash table to be sized to fit in cache. Target L2.
  const u64 l2_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L2_CACHE);
  flush_threshold_ = u64(std::llround(
      f32(l2_size) / f64(entries_.element_size()) * kDefaultLoadFactor));
  flush_threshold_ =
      std::max(u64{256}, util::MathUtil::PowerOf2Floor(flush_threshold_));
}

AggregationHashTable::~AggregationHashTable() {
  if (batch_state_ != nullptr) {
    memory_->FreeObject(std::move(batch_state_));
  }
  if (partition_heads_ != nullptr) {
    memory_->DeallocateArray(partition_heads_, kDefaultNumPartitions);
  }
  if (partition_tails_ != nullptr) {
    memory_->DeallocateArray(partition_tails_, kDefaultNumPartitions);
  }
  if (partition_estimates_ != nullptr) {
    // The estimates array uses HLL instances acquired from libcount. Libcount
    // new's HLL objects, hence, we need to manually call delete on all of them.
    // We could use unique_ptr, but then the definition and usage gets ugly, and
    // we would still need to iterate over the array to reset each unique_ptr.
    for (u32 i = 0; i < kDefaultNumPartitions; i++) {
      delete partition_estimates_[i];
    }
    memory_->DeallocateArray(partition_estimates_, kDefaultNumPartitions);
  }
  if (partition_tables_ != nullptr) {
    for (u32 i = 0; i < kDefaultNumPartitions; i++) {
      if (partition_tables_[i] != nullptr) {
        partition_tables_[i]->~AggregationHashTable();
        memory_->Deallocate(partition_tables_[i], sizeof(AggregationHashTable));
      }
    }
    memory_->DeallocateArray(partition_tables_, kDefaultNumPartitions);
  }
}

void AggregationHashTable::Grow() {
  // Resize table
  const u64 new_size = hash_table_.capacity() * 2;
  hash_table_.SetSize(new_size);
  max_fill_ =
      u64(std::llround(hash_table_.capacity() * hash_table_.load_factor()));

  // Insert elements again
  for (byte *untyped_entry : entries_) {
    auto *entry = reinterpret_cast<HashTableEntry *>(untyped_entry);
    hash_table_.Insert<false>(entry, entry->hash);
  }

  // Update stats
  stats_.num_growths++;
}

byte *AggregationHashTable::Insert(const hash_t hash) {
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

byte *AggregationHashTable::InsertPartitioned(const hash_t hash) {
  byte *ret = Insert(hash);
  if (hash_table_.num_elements() >= flush_threshold_) {
    FlushToOverflowPartitions();
  }
  return ret;
}

void AggregationHashTable::FlushToOverflowPartitions() {
  if (TPL_UNLIKELY(partition_heads_ == nullptr)) {
    AllocateOverflowPartitions();
  }

  // Dump hash table into overflow partition
  hash_table_.FlushEntries([this](HashTableEntry *entry) {
    const u64 part_idx = (entry->hash >> partition_shift_bits_);
    entry->next = partition_heads_[part_idx];
    partition_heads_[part_idx] = entry;
    if (TPL_UNLIKELY(partition_tails_[part_idx] == nullptr)) {
      partition_tails_[part_idx] = entry;
    }
    partition_estimates_[part_idx]->Update(entry->hash);
  });

  // Update stats
  stats_.num_flushes++;
}

void AggregationHashTable::AllocateOverflowPartitions() {
  TPL_ASSERT(
      (partition_heads_ == nullptr) == (partition_tails_ == nullptr),
      "Head and tail of overflow partitions list are not equally allocated");

  if (partition_heads_ == nullptr) {
    partition_heads_ =
        memory_->AllocateArray<HashTableEntry *>(kDefaultNumPartitions, true);
    partition_tails_ =
        memory_->AllocateArray<HashTableEntry *>(kDefaultNumPartitions, true);
    partition_estimates_ =
        memory_->AllocateArray<libcount::HLL *>(kDefaultNumPartitions, false);
    for (u32 i = 0; i < kDefaultNumPartitions; i++) {
      partition_estimates_[i] = libcount::HLL::Create(kDefaultHLLPrecision);
    }
    partition_tables_ = memory_->AllocateArray<AggregationHashTable *>(
        kDefaultNumPartitions, true);
  }
}

void AggregationHashTable::ProcessBatch(
    VectorProjectionIterator *iters[],
    const AggregationHashTable::VecHashFn vec_hash_fn,
    const AggregationHashTable::KeyEqFn key_eq_fn,
    const AggregationHashTable::VecKeyEqFn vec_key_eq_fn,
    const AggregationHashTable::InitAggFn init_agg_fn,
    const AggregationHashTable::VecAdvanceAggFn vec_advance_agg_fn,
    const bool partitioned) {
  TPL_ASSERT(iters[0]->num_selected() <= kDefaultVectorSize,
             "Vector projection is too large");

  // Allocate all required batch state, but only on first invocation.
  if (TPL_UNLIKELY(batch_state_ == nullptr)) {
    batch_state_ =
        memory_->NewObject<BatchProcessState>(std::unique_ptr<libcount::HLL>(
            libcount::HLL::Create(kDefaultHLLPrecision)));
  }

  // Launch
  if (iters[0]->IsFiltered()) {
    ProcessBatchImpl<true>(iters, vec_hash_fn, key_eq_fn, vec_key_eq_fn,
                           init_agg_fn, vec_advance_agg_fn, partitioned);
  } else {
    ProcessBatchImpl<false>(iters, vec_hash_fn, key_eq_fn, vec_key_eq_fn,
                            init_agg_fn, vec_advance_agg_fn, partitioned);
  }
}

template <bool VPIIsFiltered>
void AggregationHashTable::ProcessBatchImpl(
    VectorProjectionIterator *iters[],
    const AggregationHashTable::VecHashFn vec_hash_fn,
    const AggregationHashTable::KeyEqFn key_eq_fn,
    const AggregationHashTable::VecKeyEqFn vec_key_eq_fn,
    const AggregationHashTable::InitAggFn init_agg_fn,
    const AggregationHashTable::VecAdvanceAggFn vec_advance_agg_fn,
    const bool partitioned) {
  // Compute the hashes
  ComputeHash<VPIIsFiltered>(iters, vec_hash_fn);

  // Try to find associated groups for all input tuples
  const u32 found_groups = FindGroups<VPIIsFiltered>(iters, vec_key_eq_fn);
  iters[0]->Reset();

  // Create aggregates for those tuples that didn't find a match
  if (partitioned) {
    CreateMissingGroups<VPIIsFiltered, true>(iters, key_eq_fn, init_agg_fn);
  } else {
    CreateMissingGroups<VPIIsFiltered, false>(iters, key_eq_fn, init_agg_fn);
  }
  iters[0]->Reset();

  // Advance the aggregates for all tuples that did find a match
  AdvanceGroups<VPIIsFiltered>(iters, found_groups, vec_advance_agg_fn);
  iters[0]->Reset();
}

// The functions below marked NEVER_INLINE are done so on purpose. Adding them
// improves instruction locality by reducing the code size for vectorized
// components. It predictably yields 5-10% improvements to performance to
// single- and multi-threaded aggregation benchmarks.

template <bool VPIIsFiltered>
NEVER_INLINE void AggregationHashTable::ComputeHash(
    VectorProjectionIterator *iters[],
    const AggregationHashTable::VecHashFn vec_hash_fn) {
  auto &hashes = batch_state_->hashes;
  vec_hash_fn(hashes.data(), iters);
}

template <bool VPIIsFiltered>
NEVER_INLINE u32 AggregationHashTable::FindGroups(
    VectorProjectionIterator *iters[],
    const AggregationHashTable::VecKeyEqFn vec_key_eq_fn) {
  batch_state_->key_not_eq.clear();
  batch_state_->groups_not_found.clear();
  u32 found = LookupInitial(iters[0]->num_selected());
  u32 keys_equal = CheckKeyEquality<VPIIsFiltered>(iters, found, vec_key_eq_fn);
  while (!batch_state_->key_not_eq.empty()) {
    found = FollowNext();
    if (found == 0) break;
    batch_state_->key_not_eq.clear();
    keys_equal += CheckKeyEquality<VPIIsFiltered>(iters, found, vec_key_eq_fn);
  }
  return keys_equal;
}

NEVER_INLINE u32 AggregationHashTable::LookupInitial(const u32 num_elems) {
  // If the hash table is larger than cache, inject prefetch instructions
  u64 l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (hash_table_.GetTotalMemoryUsage() > l3_cache_size) {
    return LookupInitialImpl<true>(num_elems);
  } else {  // NOLINT
    return LookupInitialImpl<false>(num_elems);
  }
}

// This function will perform an initial lookup for all input tuples, storing
// entries that match on hash value in the 'entries' array.
template <bool Prefetch>
NEVER_INLINE u32 AggregationHashTable::LookupInitialImpl(const u32 num_elems) {
  auto &hashes = batch_state_->hashes;
  auto &entries = batch_state_->entries;
  auto &groups_found = batch_state_->groups_found;
  auto &groups_not_found = batch_state_->groups_not_found;

  u32 found = 0;
  for (u32 idx = 0, prefetch_idx = kPrefetchDistance; idx < num_elems;) {
    if constexpr (Prefetch) {
      if (TPL_LIKELY(prefetch_idx < num_elems)) {
        hash_table_.PrefetchChainHead<false>(hashes[prefetch_idx++]);
      }
    }

    const auto hash = hashes[idx];
    if (auto *entry = hash_table_.FindChainHead(hash)) {
      if (entry->hash == hash) {
        entries[idx] = entry;
        groups_found[found++] = idx;
        goto nextChain;
      }
      for (entry = entry->next; entry != nullptr; entry = entry->next) {
        if (entry->hash == hash) {
          entries[idx] = entry;
          groups_found[found++] = idx;
          goto nextChain;
        }
      }
    }
    groups_not_found.append(idx);
  nextChain:
    idx++;
  }

  return found;
}

// For all entries in 'groups_found', check whether their keys match. After this
// function, groups_found will densely contain the indexes of matching keys,
// and 'key_not_eq' will contain indexes of tuples that didn't match on key.
template <bool VPIIsFiltered>
NEVER_INLINE u32 AggregationHashTable::CheckKeyEquality(
    VectorProjectionIterator *iters[], const u32 num_elems,
    const AggregationHashTable::VecKeyEqFn vec_key_eq_fn) {
  auto &entries = batch_state_->entries;
  auto &groups_found = batch_state_->groups_found;
  auto &key_not_eq = batch_state_->key_not_eq;

  u32 matched = 0;
  // Faster, but uses the stack
  std::array<byte *, kDefaultVectorSize> payloads;
  std::array<bool, kDefaultVectorSize> matches{};
  for (u32 i = 0; i < num_elems; i++) {
    const u32 index = groups_found[i];
    // Retrieve the aggregate we're about to compare keys with
    HashTableEntry *entry = entries[index];
    // Add it to the list of payloads
    payloads[i] = entry->payload;
  }
  vec_key_eq_fn(payloads.data(), iters, groups_found.data(), matches.data(),
                num_elems);
  // Iterator through matches
  for (u32 i = 0; i < num_elems; i++) {
    // Check if there is a match at this index
    if (matches[i]) {
      groups_found[matched++] = groups_found[i];
    } else {
      key_not_eq.append(groups_found[i]);
    }
  }
  return matched;
}

// For all unmatched keys, move along the chain until we find another hash
// match. After this function, 'groups_found' will densely contain the indexes
// of tuples that matched on hash and should be checked for keys.
NEVER_INLINE u32 AggregationHashTable::FollowNext() {
  const auto &hashes = batch_state_->hashes;
  const auto &key_not_eq = batch_state_->key_not_eq;
  auto &entries = batch_state_->entries;
  auto &groups_found = batch_state_->groups_found;
  auto &groups_not_found = batch_state_->groups_not_found;

  u32 matched = 0;
  for (u32 i = 0; i < key_not_eq.size();) {
    const auto index = key_not_eq[i];
    for (auto *entry = entries[index]; entry != nullptr; entry = entry->next) {
      const auto hash = hashes[index];
      if (entry->hash == hash) {
        entries[index] = entry;
        groups_found[matched++] = index;
        goto nextChain;
      }
    }
    groups_not_found.append(index);
  nextChain:
    i++;
  }
  return matched;
}

template <bool VPIIsFiltered, bool Partitioned>
NEVER_INLINE void AggregationHashTable::CreateMissingGroups(
    VectorProjectionIterator *iters[],
    const AggregationHashTable::KeyEqFn key_eq_fn,
    const AggregationHashTable::InitAggFn init_agg_fn) {
  const auto &hashes = batch_state_->hashes;
  const auto &groups_not_found = batch_state_->groups_not_found;
  auto &entries = batch_state_->entries;
  for (u32 i = 0; i < groups_not_found.size(); i++) {
    const auto index = groups_not_found[i];

    // Position the iterator to the tuple we're inserting
    iters[0]->SetPosition<VPIIsFiltered>(index);

    // But before we insert, check if already inserted
    const hash_t hash = hashes[index];
    if (auto *entry = LookupEntryInternal(hash, key_eq_fn, iters)) {
      entries[index] = entry;
      continue;
    }
    // Initialize
    if constexpr (Partitioned) {
      init_agg_fn(InsertPartitioned(hash), iters);
    } else {  // NOLINT
      init_agg_fn(Insert(hash), iters);
    }
  }
}

template <bool VPIIsFiltered>
NEVER_INLINE void AggregationHashTable::AdvanceGroups(
    VectorProjectionIterator *iters[], const u32 num_groups,
    const AggregationHashTable::VecAdvanceAggFn vec_advance_agg_fn) {
  const auto &entries = batch_state_->entries;
  const auto &groups_found = batch_state_->groups_found;

  std::array<std::byte *, kDefaultVectorSize> payloads;
  for (u32 i = 0; i < num_groups; i++) {
    const auto index = groups_found[i];
    // The entry storing the aggregate we'll update
    HashTableEntry *entry = entries[index];
    payloads[i] = entry->payload;
  }
  vec_advance_agg_fn(payloads.data(), iters, groups_found.data(), num_groups);
}

void AggregationHashTable::TransferMemoryAndPartitions(
    ThreadStateContainer *const thread_states, const std::size_t agg_ht_offset,
    const AggregationHashTable::MergePartitionFn merge_partition_fn) {
  // Set the partition merging function. This function tells us how to merge
  // a set of overflow partitions into an AggregationHashTable.
  merge_partition_fn_ = merge_partition_fn;

  // Allocate the set of overflow partitions so that we can link in all
  // thread-local overflow partitions to us.
  AllocateOverflowPartitions();

  // If, by chance, we have some unflushed aggregate data, flush it out now to
  // ensure partitioned build captures it.
  if (NumElements() > 0) {
    FlushToOverflowPartitions();
  }

  // Okay, now we actually pull out the thread-local aggregation hash tables and
  // move both their main entry data and the overflow partitions to us.
  std::vector<AggregationHashTable *> tl_agg_ht;
  thread_states->CollectThreadLocalStateElementsAs(tl_agg_ht, agg_ht_offset);

  for (auto *table : tl_agg_ht) {
    // Flush each table to ensure their hash tables are empty and their
    // overflow partitions contain all partial aggregates
    table->FlushToOverflowPartitions();

    // Now, move over their memory
    owned_entries_.emplace_back(std::move(table->entries_));

    TPL_ASSERT(
        table->owned_entries_.empty(),
        "A thread-local aggregation table should not have any owned "
        "entries themselves. Nested/recursive aggregations not supported.");

    // Now, move over their overflow partitions list
    for (u32 part_idx = 0; part_idx < kDefaultNumPartitions; part_idx++) {
      if (table->partition_heads_[part_idx] != nullptr) {
        // Link in the partition list
        table->partition_tails_[part_idx]->next = partition_heads_[part_idx];
        partition_heads_[part_idx] = table->partition_heads_[part_idx];
        if (partition_tails_[part_idx] == nullptr) {
          partition_tails_[part_idx] = table->partition_tails_[part_idx];
        }
        // Update the partition's unique-count estimate
        partition_estimates_[part_idx]->Merge(
            table->partition_estimates_[part_idx]);
      }
    }
  }
}

AggregationHashTable *AggregationHashTable::BuildTableOverPartition(
    void *const query_state, const u32 partition_idx) {
  TPL_ASSERT(partition_idx < kDefaultNumPartitions,
             "Out-of-bounds partition access");
  TPL_ASSERT(partition_heads_[partition_idx] != nullptr,
             "Should not build aggregation table over empty partition!");

  // If the table has already been built, return it
  if (partition_tables_[partition_idx] != nullptr) {
    return partition_tables_[partition_idx];
  }

  // Create it
  auto estimated_size = partition_estimates_[partition_idx]->Estimate();
  auto *agg_table = new (memory_->AllocateAligned(
      sizeof(AggregationHashTable), alignof(AggregationHashTable), false))
      AggregationHashTable(memory_, payload_size_,
                           static_cast<u32>(estimated_size));

  util::Timer<std::milli> timer;
  timer.Start();

  // Build it
  AHTOverflowPartitionIterator iter(partition_heads_ + partition_idx,
                                    partition_heads_ + partition_idx + 1);
  merge_partition_fn_(query_state, agg_table, &iter);

  timer.Stop();
  LOG_DEBUG(
      "Overflow Partition {}: estimated size = {}, actual size = {}, "
      "build time = {:2f} ms",
      partition_idx, estimated_size, agg_table->NumElements(), timer.elapsed());

  // Set it
  partition_tables_[partition_idx] = agg_table;

  // Return it
  return agg_table;
}

void AggregationHashTable::ExecuteParallelPartitionedScan(
    void *query_state, ThreadStateContainer *thread_states,
    AggregationHashTable::ScanPartitionFn scan_fn) {
  //
  // At this point, this aggregation table has a list of overflow partitions
  // that must be merged into a single aggregation hash table. For simplicity,
  // we create a new aggregation hash table per overflow partition, and merge
  // the contents of that partition into the new hash table. We use the HLL
  // estimates to size the hash table before construction so as to minimize the
  // growth factor. Each aggregation hash table partition will be built and
  // scanned in parallel.
  //

  TPL_ASSERT(partition_heads_ != nullptr && merge_partition_fn_ != nullptr,
             "No overflow partitions allocated, or no merging function "
             "allocated. Did you call TransferMemoryAndPartitions() before "
             "issuing the partitioned scan?");

  // Determine the non-empty overflow partitions
  alignas(CACHELINE_SIZE) u32 nonempty_parts[kDefaultNumPartitions];
  const u32 num_nonempty_parts = util::VectorUtil::SelectNotNull(
      partition_heads_, kDefaultNumPartitions, nonempty_parts, nullptr);

  tbb::parallel_for_each(
      nonempty_parts, nonempty_parts + num_nonempty_parts,
      [&](const u32 part_idx) {
        // Build a hash table over the given partition
        auto *agg_table_part = BuildTableOverPartition(query_state, part_idx);

        // Get a handle to the thread-local state of the executing thread
        auto *thread_state = thread_states->AccessThreadStateOfCurrentThread();

        // Scan the partition
        scan_fn(query_state, thread_state, agg_table_part);
      });
}

}  // namespace tpl::sql
