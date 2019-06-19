#include "sql/aggregation_hash_table.h"

#include <algorithm>
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
      batch_process_state_(nullptr),
      merge_partition_fn_(nullptr),
      partition_heads_(nullptr),
      partition_tails_(nullptr),
      partition_estimates_(nullptr),
      partition_tables_(nullptr),
      partition_shift_bits_(
          util::BitUtil::CountLeadingZeros(u64(kDefaultNumPartitions) - 1)) {
  hash_table_.SetSize(initial_size);
  max_fill_ = std::llround(hash_table_.capacity() * hash_table_.load_factor());

  // Compute flush threshold. In partitioned mode, we want the thread-local
  // pre-aggregation hash table to be sized to fit in cache. Target L2.
  const u64 l2_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L2_CACHE);
  flush_threshold_ =
      std::llround(f32(l2_size) / entries_.element_size() * kDefaultLoadFactor);
  flush_threshold_ =
      std::max(256ul, util::MathUtil::PowerOf2Floor(flush_threshold_));
}

AggregationHashTable::~AggregationHashTable() {
  if (batch_process_state_ != nullptr) {
    memory_->Deallocate(batch_process_state_, sizeof(BatchProcessState));
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
  max_fill_ = std::llround(hash_table_.capacity() * hash_table_.load_factor());

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
    const AggregationHashTable::HashFn hash_fn,
    const AggregationHashTable::KeyEqFn key_eq_fn,
    const AggregationHashTable::InitAggFn init_agg_fn,
    const AggregationHashTable::AdvanceAggFn advance_agg_fn,
    const bool partitioned) {
  if (TPL_UNLIKELY(batch_process_state_ == nullptr)) {
    batch_process_state_ = static_cast<BatchProcessState *>(
        memory_->Allocate(sizeof(BatchProcessState), false));
  }

  const u32 num_elems = iters[0]->num_selected();

  auto *const hashes = batch_process_state_->hashes;
  auto *const entries = batch_process_state_->entries;

  if (iters[0]->IsFiltered()) {
    ProcessBatchImpl<true>(iters, num_elems, hashes, entries, hash_fn,
                           key_eq_fn, init_agg_fn, advance_agg_fn, partitioned);
  } else {
    ProcessBatchImpl<false>(iters, num_elems, hashes, entries, hash_fn,
                            key_eq_fn, init_agg_fn, advance_agg_fn,
                            partitioned);
  }
}

template <bool VPIIsFiltered>
void AggregationHashTable::ProcessBatchImpl(
    VectorProjectionIterator *iters[], u32 num_elems, hash_t hashes[],
    HashTableEntry *entries[], const AggregationHashTable::HashFn hash_fn,
    const AggregationHashTable::KeyEqFn key_eq_fn,
    const AggregationHashTable::InitAggFn init_agg_fn,
    const AggregationHashTable::AdvanceAggFn advance_agg_fn,
    const bool partitioned) {
  // Lookup the groups for all available input tuples
  LookupGroups<VPIIsFiltered>(iters, num_elems, hashes, entries, hash_fn,
                              key_eq_fn);
  iters[0]->Reset();

  // Now, the entries vector contains a NULL pointer for missing groups. Create
  // them now.
  if (partitioned) {
    CreateMissingGroups<VPIIsFiltered, true>(iters, num_elems, hashes, entries,
                                             key_eq_fn, init_agg_fn);
  } else {
    CreateMissingGroups<VPIIsFiltered, false>(iters, num_elems, hashes, entries,
                                              key_eq_fn, init_agg_fn);
  }
  iters[0]->Reset();

  // For the remaining non-null groups, update/advance them
  AdvanceGroups<VPIIsFiltered>(iters, num_elems, entries, advance_agg_fn);
  iters[0]->Reset();
}

template <bool VPIIsFiltered>
void AggregationHashTable::LookupGroups(
    VectorProjectionIterator *iters[], const u32 num_elems, hash_t hashes[],
    HashTableEntry *entries[], const AggregationHashTable::HashFn hash_fn,
    const AggregationHashTable::KeyEqFn key_eq_fn) {
  // Compute hash and perform initial lookup
  ComputeHashAndLoadInitial<VPIIsFiltered>(iters, num_elems, hashes, entries,
                                           hash_fn);

  // Determine the indexes of entries that are non-null
  u32 *const group_sel = batch_process_state_->group_sel;
  const u32 num_groups =
      util::VectorUtil::SelectNotNull(entries, num_elems, group_sel, nullptr);

  // Candidate groups in 'entries' may have hash collisions. Follow the chain
  // to check key equality.
  FollowNextLoop<VPIIsFiltered>(iters, num_groups, group_sel, hashes, entries,
                                key_eq_fn);
}

template <bool VPIIsFiltered>
void AggregationHashTable::ComputeHashAndLoadInitial(
    VectorProjectionIterator *iters[], const u32 num_elems, hash_t hashes[],
    HashTableEntry *entries[],
    const AggregationHashTable::HashFn hash_fn) const {
  // If the hash table is larger than cache, inject prefetch instructions
  u64 l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (hash_table_.GetTotalMemoryUsage() > l3_cache_size) {
    ComputeHashAndLoadInitialImpl<VPIIsFiltered, true>(iters, num_elems, hashes,
                                                       entries, hash_fn);
  } else {
    ComputeHashAndLoadInitialImpl<VPIIsFiltered, false>(
        iters, num_elems, hashes, entries, hash_fn);
  }
}

template <bool VPIIsFiltered, bool Prefetch>
void AggregationHashTable::ComputeHashAndLoadInitialImpl(
    VectorProjectionIterator *iters[], const u32 num_elems, hash_t hashes[],
    HashTableEntry *entries[],
    const AggregationHashTable::HashFn hash_fn) const {
  // Compute hash
  if constexpr (VPIIsFiltered) {
    for (u32 idx = 0; iters[0]->HasNextFiltered();
         iters[0]->AdvanceFiltered()) {
      hashes[idx++] = hash_fn(iters);
    }
  } else {
    for (u32 idx = 0; iters[0]->HasNext(); iters[0]->Advance()) {
      hashes[idx++] = hash_fn(iters);
    }
  }

  // Reset VPI
  iters[0]->Reset();

  // Load entries
  for (u32 idx = 0, prefetch_idx = kPrefetchDistance; idx < num_elems;
       idx++, prefetch_idx++) {
    if constexpr (Prefetch) {
      if (TPL_LIKELY(prefetch_idx < num_elems)) {
        hash_table_.PrefetchChainHead<false>(hashes[prefetch_idx]);
      }
    }
    // Load chain head
    entries[idx] = hash_table_.FindChainHead(hashes[idx]);
  }
}

template <bool VPIIsFiltered>
void AggregationHashTable::FollowNextLoop(
    VectorProjectionIterator *iters[], u32 num_elems, u32 group_sel[],
    const hash_t hashes[], HashTableEntry *entries[],
    const AggregationHashTable::KeyEqFn key_eq_fn) const {
  while (num_elems > 0) {
    u32 write_idx = 0;

    // Simultaneously iterate over valid groups and input probe tuples in the
    // vector projection and check key equality for each. For mismatches,
    // follow the bucket chain.
    for (u32 idx = 0; idx < num_elems; idx++) {
      iters[0]->SetPosition<VPIIsFiltered>(group_sel[idx]);

      const bool keys_match =
          entries[group_sel[idx]]->hash == hashes[group_sel[idx]] &&
          key_eq_fn(entries[group_sel[idx]]->payload, iters);
      const bool has_next = entries[group_sel[idx]]->next != nullptr;

      group_sel[write_idx] = group_sel[idx];
      write_idx += static_cast<u32>(!keys_match && has_next);
    }

    // Reset VPI
    iters[0]->Reset();

    // Follow chain
    for (u32 idx = 0; idx < write_idx; idx++) {
      HashTableEntry *&entry = entries[group_sel[idx]];
      entry = entry->next;
    }

    // Next
    num_elems = write_idx;
  }
}

template <bool VPIIsFiltered, bool Partitioned>
void AggregationHashTable::CreateMissingGroups(
    VectorProjectionIterator *iters[], u32 num_elems, const hash_t hashes[],
    HashTableEntry *entries[], const AggregationHashTable::KeyEqFn key_eq_fn,
    const AggregationHashTable::InitAggFn init_agg_fn) {
  // Determine which elements are missing a group
  u32 *const group_sel = batch_process_state_->group_sel;
  const u32 num_groups =
      util::VectorUtil::SelectNull(entries, num_elems, group_sel, nullptr);

  // Insert those elements
  for (u32 idx = 0; idx < num_groups; idx++) {
    const hash_t hash = hashes[group_sel[idx]];

    // Move VPI to position of new aggregate
    iters[0]->SetPosition<VPIIsFiltered>(group_sel[idx]);

    if (auto *entry = LookupEntryInternal(hash, key_eq_fn, iters)) {
      entries[group_sel[idx]] = entry;
      continue;
    }

    // Initialize
    if constexpr (Partitioned) {
      init_agg_fn(InsertPartitioned(hash), iters);
    } else {
      init_agg_fn(Insert(hash), iters);
    }
  }
}

template <bool VPIIsFiltered>
void AggregationHashTable::AdvanceGroups(
    VectorProjectionIterator *iters[], u32 num_elems, HashTableEntry *entries[],
    const AggregationHashTable::AdvanceAggFn advance_agg_fn) {
  // All non-null entries are groups that should be updated. Find them now.
  u32 *const group_sel = batch_process_state_->group_sel;
  const u32 num_groups =
      util::VectorUtil::SelectNotNull(entries, num_elems, group_sel, nullptr);

  // Group indexes are stored in group_sel, update them now.
  for (u32 idx = 0; idx < num_groups; idx++) {
    HashTableEntry *entry = entries[group_sel[idx]];
    iters[0]->SetPosition<VPIIsFiltered>(group_sel[idx]);
    advance_agg_fn(entry->payload, iters);
  }
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
      AggregationHashTable(memory_, payload_size_, estimated_size);

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
