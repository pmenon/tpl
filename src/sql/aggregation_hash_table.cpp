#include "sql/aggregation_hash_table.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "tbb/tbb.h"

#include "count/hll.h"

#include "common/cpu_info.h"
#include "logging/logger.h"
#include "sql/thread_state_container.h"
#include "sql/vector_projection_iterator.h"
#include "util/bit_util.h"
#include "util/math_util.h"
#include "util/timer.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Batch Process State
// ---------------------------------------------------------

AggregationHashTable::BatchProcessState::BatchProcessState(std::unique_ptr<libcount::HLL> estimator)
    : hll_estimator(std::move(estimator)), hashes{0}, entries{nullptr}, groups_found{0} {}

AggregationHashTable::BatchProcessState::~BatchProcessState() = default;

// ---------------------------------------------------------
// Aggregation Hash Table
// ---------------------------------------------------------

AggregationHashTable::AggregationHashTable(MemoryPool *memory, const std::size_t payload_size,
                                           const uint32_t initial_size)
    : memory_(memory),
      payload_size_(payload_size),
      entries_(HashTableEntry::ComputeEntrySize(payload_size_), MemoryPoolAllocator<byte>(memory_)),
      owned_entries_(memory_),
      hash_table_(kDefaultLoadFactor),
      batch_state_(nullptr),
      merge_partition_fn_(nullptr),
      partition_heads_(nullptr),
      partition_tails_(nullptr),
      partition_estimates_(nullptr),
      partition_tables_(nullptr),
      partition_shift_bits_(util::BitUtil::CountLeadingZeros(uint64_t(kDefaultNumPartitions) - 1)) {
  hash_table_.SetSize(initial_size);
  max_fill_ = std::llround(hash_table_.GetCapacity() * hash_table_.GetLoadFactor());

  // Compute flush threshold. In partitioned mode, we want the thread-local
  // pre-aggregation hash table to be sized to fit in cache. Target L2.
  const uint64_t l2_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L2_CACHE);
  flush_threshold_ =
      std::llround(static_cast<float>(l2_size) / entries_.element_size() * kDefaultLoadFactor);
  flush_threshold_ = std::max(uint64_t{256}, util::MathUtil::PowerOf2Floor(flush_threshold_));
}

AggregationHashTable::AggregationHashTable(MemoryPool *memory, std::size_t payload_size)
    : AggregationHashTable(memory, payload_size, kDefaultInitialTableSize) {}

AggregationHashTable::~AggregationHashTable() {
  if (batch_state_ != nullptr) {
    memory_->DeleteObject(std::move(batch_state_));
  }
  if (partition_heads_ != nullptr) {
    memory_->DeallocateArray(partition_heads_, kDefaultNumPartitions);
  }
  if (partition_tails_ != nullptr) {
    memory_->DeallocateArray(partition_tails_, kDefaultNumPartitions);
  }
  if (partition_estimates_ != nullptr) {
    // The estimates array uses HLL instances acquired from libcount. We own them so we have to
    // delete them manually.
    for (uint32_t i = 0; i < kDefaultNumPartitions; i++) {
      delete partition_estimates_[i];
    }
    memory_->DeallocateArray(partition_estimates_, kDefaultNumPartitions);
  }
  if (partition_tables_ != nullptr) {
    for (uint32_t i = 0; i < kDefaultNumPartitions; i++) {
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
  const uint64_t new_size = hash_table_.GetCapacity() * 2;
  hash_table_.SetSize(new_size);
  max_fill_ = std::llround(hash_table_.GetCapacity() * hash_table_.GetLoadFactor());

  // Insert elements again
  for (byte *untyped_entry : entries_) {
    auto *entry = reinterpret_cast<HashTableEntry *>(untyped_entry);
    hash_table_.Insert<false>(entry);
  }

  // Update stats
  stats_.num_growths++;
}

byte *AggregationHashTable::AllocInputTuple(hash_t hash) {
  // Grow if need be
  if (NeedsToGrow()) {
    Grow();
  }

  // Allocate an entry
  auto *entry = reinterpret_cast<HashTableEntry *>(entries_.append());
  entry->hash = hash;
  entry->next = nullptr;

  // Insert into table
  hash_table_.Insert<false>(entry);

  // Give the payload so the client can write into it
  return entry->payload;
}

void AggregationHashTable::AllocateOverflowPartitions() {
  TPL_ASSERT((partition_heads_ == nullptr) == (partition_tails_ == nullptr),
             "Head and tail of overflow partitions list are not equally allocated");

  if (partition_heads_ == nullptr) {
    partition_heads_ = memory_->AllocateArray<HashTableEntry *>(kDefaultNumPartitions, true);
    partition_tails_ = memory_->AllocateArray<HashTableEntry *>(kDefaultNumPartitions, true);
    partition_estimates_ = memory_->AllocateArray<libcount::HLL *>(kDefaultNumPartitions, false);
    for (uint32_t i = 0; i < kDefaultNumPartitions; i++) {
      partition_estimates_[i] = libcount::HLL::Create(kDefaultHLLPrecision).release();
    }
    partition_tables_ = memory_->AllocateArray<AggregationHashTable *>(kDefaultNumPartitions, true);
  }
}

void AggregationHashTable::FlushToOverflowPartitions() {
  if (TPL_UNLIKELY(partition_heads_ == nullptr)) {
    AllocateOverflowPartitions();
  }

  // Dump all entries from the hash table into the overflow partitions. For each
  // entry in the table, we compute its destination partition using the highest
  // log(P) bits for P partitions. For each partition, we also track an estimate
  // of the number of unique hash values so that when the partitions are merged,
  // we can appropriately size the table before merging. The issue is that both
  // the bits used for partition selection and unique hash estimation are the
  // same! Thus, the estimates are inaccurate. To solve this, we scramble the
  // hash values using a bijective hash scrambling before feeding them to the
  // estimator.

  hash_table_.FlushEntries([this](HashTableEntry *entry) {
    const uint64_t partition_idx = (entry->hash >> partition_shift_bits_);
    entry->next = partition_heads_[partition_idx];
    partition_heads_[partition_idx] = entry;
    if (TPL_UNLIKELY(partition_tails_[partition_idx] == nullptr)) {
      partition_tails_[partition_idx] = entry;
    }
    partition_estimates_[partition_idx]->Update(util::HashUtil::ScrambleHash(entry->hash));
  });

  // Update stats
  stats_.num_flushes++;
}

byte *AggregationHashTable::AllocInputTuplePartitioned(hash_t hash) {
  byte *ret = AllocInputTuple(hash);
  if (hash_table_.GetElementCount() >= flush_threshold_) {
    FlushToOverflowPartitions();
  }
  return ret;
}

void AggregationHashTable::ProcessBatch(VectorProjectionIterator *vpi,
                                        const AggregationHashTable::HashFn hash_fn,
                                        const AggregationHashTable::KeyEqFn key_eq_fn,
                                        const AggregationHashTable::InitAggFn init_agg_fn,
                                        const AggregationHashTable::AdvanceAggFn advance_agg_fn,
                                        const bool partitioned) {
  TPL_ASSERT(vpi->GetTupleCount() <= kDefaultVectorSize, "Vector projection is too large");

  // Allocate all required batch state, but only on first invocation.
  if (TPL_UNLIKELY(batch_state_ == nullptr)) {
    batch_state_ =
        memory_->MakeObject<BatchProcessState>(libcount::HLL::Create(kDefaultHLLPrecision));
  }

  // Launch
  if (vpi->IsFiltered()) {
    ProcessBatchImpl<true>(vpi, hash_fn, key_eq_fn, init_agg_fn, advance_agg_fn, partitioned);
  } else {
    ProcessBatchImpl<false>(vpi, hash_fn, key_eq_fn, init_agg_fn, advance_agg_fn, partitioned);
  }
}

template <bool VPIIsFiltered>
void AggregationHashTable::ProcessBatchImpl(VectorProjectionIterator *vpi,
                                            const AggregationHashTable::HashFn hash_fn,
                                            const AggregationHashTable::KeyEqFn key_eq_fn,
                                            const AggregationHashTable::InitAggFn init_agg_fn,
                                            const AggregationHashTable::AdvanceAggFn advance_agg_fn,
                                            const bool partitioned) {
  // Compute the hashes
  ComputeHash<VPIIsFiltered>(vpi, hash_fn);

  // Try to find associated groups for all input tuples
  const uint32_t found_groups = FindGroups<VPIIsFiltered>(vpi, key_eq_fn);
  vpi->Reset();

  // Create aggregates for those tuples that didn't find a match
  if (partitioned) {
    CreateMissingGroups<VPIIsFiltered, true>(vpi, key_eq_fn, init_agg_fn);
  } else {
    CreateMissingGroups<VPIIsFiltered, false>(vpi, key_eq_fn, init_agg_fn);
  }
  vpi->Reset();

  // Advance the aggregates for all tuples that did find a match
  AdvanceGroups<VPIIsFiltered>(vpi, found_groups, advance_agg_fn);
  vpi->Reset();
}

template <bool VPIIsFiltered>
void AggregationHashTable::ComputeHash(VectorProjectionIterator *vpi,
                                       const AggregationHashTable::HashFn hash_fn) {
  auto &hashes = batch_state_->hashes;
  if constexpr (VPIIsFiltered) {
    for (uint32_t idx = 0; vpi->HasNextFiltered(); vpi->AdvanceFiltered()) {
      hashes[idx++] = hash_fn(vpi);
    }
  } else {  // NOLINT
    for (uint32_t idx = 0; vpi->HasNext(); vpi->Advance()) {
      hashes[idx++] = hash_fn(vpi);
    }
  }
}

template <bool VPIIsFiltered>
uint32_t AggregationHashTable::FindGroups(VectorProjectionIterator *vpi,
                                          const AggregationHashTable::KeyEqFn key_eq_fn) {
  batch_state_->key_not_eq.clear();
  batch_state_->groups_not_found.clear();
  uint32_t found = LookupInitial(vpi->GetTupleCount());
  uint32_t keys_equal = CheckKeyEquality<VPIIsFiltered>(vpi, found, key_eq_fn);
  while (!batch_state_->key_not_eq.empty()) {
    found = FollowNext();
    if (found == 0) break;
    batch_state_->key_not_eq.clear();
    keys_equal += CheckKeyEquality<VPIIsFiltered>(vpi, found, key_eq_fn);
  }
  return keys_equal;
}

uint32_t AggregationHashTable::LookupInitial(const uint32_t num_elems) {
  // If the hash table is larger than cache, inject prefetch instructions
  uint64_t l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (hash_table_.GetTotalMemoryUsage() > l3_cache_size) {
    return LookupInitialImpl<true>(num_elems);
  } else {  // NOLINT
    return LookupInitialImpl<false>(num_elems);
  }
}

// This function will perform an initial lookup for all input tuples, storing
// entries that match on hash value in the 'entries' array.
template <bool Prefetch>
uint32_t AggregationHashTable::LookupInitialImpl(const uint32_t num_elems) {
  auto &hashes = batch_state_->hashes;
  auto &entries = batch_state_->entries;
  auto &groups_found = batch_state_->groups_found;
  auto &groups_not_found = batch_state_->groups_not_found;

  uint32_t found = 0;
  for (uint32_t idx = 0, prefetch_idx = kPrefetchDistance; idx < num_elems;) {
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
// function, groups_found will densely contain the indexes of matching keys, and
// 'key_not_eq' will contain indexes of tuples that didn't match on key.
template <bool VPIIsFiltered>
uint32_t AggregationHashTable::CheckKeyEquality(VectorProjectionIterator *vpi, uint32_t num_elems,
                                                const AggregationHashTable::KeyEqFn key_eq_fn) {
  auto &entries = batch_state_->entries;
  auto &groups_found = batch_state_->groups_found;
  auto &key_not_eq = batch_state_->key_not_eq;

  uint32_t matched = 0;
  for (uint32_t i = 0; i < num_elems; i++) {
    const uint32_t index = groups_found[i];

    // Position the iterator on the tuple we're about the compare keys with
    vpi->SetPosition<VPIIsFiltered>(index);

    // Compare keys of the aggregate and the input tuple
    if (key_eq_fn(entries[index]->payload, vpi)) {
      groups_found[matched++] = index;
    } else {
      key_not_eq.append(index);
    }
  }

  return matched;
}

// For all unmatched keys, move along the chain until we find another hash
// match. After this function, 'groups_found' will densely contain the indexes
// of tuples that matched on hash and should be checked for keys.
uint32_t AggregationHashTable::FollowNext() {
  const auto &hashes = batch_state_->hashes;
  const auto &key_not_eq = batch_state_->key_not_eq;
  auto &entries = batch_state_->entries;
  auto &groups_found = batch_state_->groups_found;
  auto &groups_not_found = batch_state_->groups_not_found;

  uint32_t matched = 0;
  for (uint32_t i = 0; i < key_not_eq.size();) {
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
void AggregationHashTable::CreateMissingGroups(VectorProjectionIterator *vpi,
                                               const AggregationHashTable::KeyEqFn key_eq_fn,
                                               const AggregationHashTable::InitAggFn init_agg_fn) {
  const auto &hashes = batch_state_->hashes;
  const auto &groups_not_found = batch_state_->groups_not_found;
  auto &entries = batch_state_->entries;

  for (uint64_t i = 0; i < groups_not_found.size(); i++) {
    const auto index = groups_not_found[i];

    // Position the iterator to the tuple we're inserting
    vpi->SetPosition<VPIIsFiltered>(index);

    // But before we insert, check if already inserted
    const hash_t hash = hashes[index];
    if (auto *entry = LookupEntryInternal(hash, key_eq_fn, vpi)) {
      entries[index] = entry;
      continue;
    }

    // Initialize
    init_agg_fn(Partitioned ? AllocInputTuplePartitioned(hash) : AllocInputTuple(hash), vpi);
  }
}

template <bool VPIIsFiltered>
void AggregationHashTable::AdvanceGroups(VectorProjectionIterator *vpi, const uint32_t num_groups,
                                         const AggregationHashTable::AdvanceAggFn advance_agg_fn) {
  const auto &entries = batch_state_->entries;
  const auto &groups_found = batch_state_->groups_found;

  for (uint32_t i = 0; i < num_groups; i++) {
    const auto index = groups_found[i];

    // Position the iterator at the tuple we'll use to update the aggregate
    vpi->SetPosition<VPIIsFiltered>(index);

    // Call the advancement function
    advance_agg_fn(entries[index]->payload, vpi);
  }
}

void AggregationHashTable::TransferMemoryAndPartitions(
    ThreadStateContainer *thread_states, const std::size_t agg_ht_offset,
    const AggregationHashTable::MergePartitionFn merge_partition_fn) {
  // Set the partition merging function. This function tells us how to merge a
  // set of overflow partitions into an AggregationHashTable.
  merge_partition_fn_ = merge_partition_fn;

  // Allocate the set of overflow partitions so that we can link in all
  // thread-local overflow partitions to us.
  AllocateOverflowPartitions();

  // If, by chance, we have some un-flushed aggregate data, flush it out now to
  // ensure partitioned build captures it.
  if (GetTupleCount() > 0) {
    FlushToOverflowPartitions();
  }

  // Okay, now we actually pull out the thread-local aggregation hash tables and
  // move both their main entry data and the overflow partitions to us.
  std::vector<AggregationHashTable *> tl_agg_ht;
  thread_states->CollectThreadLocalStateElementsAs(tl_agg_ht, agg_ht_offset);

  for (auto *table : tl_agg_ht) {
    // Flush each table to ensure their hash tables are empty and their overflow
    // partitions contain all partial aggregates
    table->FlushToOverflowPartitions();

    // Now, move over their memory
    owned_entries_.emplace_back(std::move(table->entries_));

    TPL_ASSERT(table->owned_entries_.empty(),
               "A thread-local aggregation table should not have any owned "
               "entries themselves. Nested/recursive aggregations not supported.");

    // Now, move over their overflow partitions list
    for (uint32_t part_idx = 0; part_idx < kDefaultNumPartitions; part_idx++) {
      if (table->partition_heads_[part_idx] != nullptr) {
        // Link in the partition list
        table->partition_tails_[part_idx]->next = partition_heads_[part_idx];
        partition_heads_[part_idx] = table->partition_heads_[part_idx];
        if (partition_tails_[part_idx] == nullptr) {
          partition_tails_[part_idx] = table->partition_tails_[part_idx];
        }
        // Update the partition's unique-count estimate
        partition_estimates_[part_idx]->Merge(table->partition_estimates_[part_idx]);
      }
    }
  }
}

AggregationHashTable *AggregationHashTable::BuildTableOverPartition(void *const query_state,
                                                                    const uint32_t partition_idx) {
  TPL_ASSERT(partition_idx < kDefaultNumPartitions, "Out-of-bounds partition access");
  TPL_ASSERT(partition_heads_[partition_idx] != nullptr,
             "Should not build aggregation table over empty partition!");

  // If the table has already been built, return it
  if (partition_tables_[partition_idx] != nullptr) {
    return partition_tables_[partition_idx];
  }

  // Create it
  auto estimated_size = partition_estimates_[partition_idx]->Estimate();
  auto *agg_table = new (
      memory_->AllocateAligned(sizeof(AggregationHashTable), alignof(AggregationHashTable), false))
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
      partition_idx, estimated_size, agg_table->GetTupleCount(), timer.GetElapsed());

  // Set it
  partition_tables_[partition_idx] = agg_table;

  // Return it
  return agg_table;
}

void AggregationHashTable::ExecuteParallelPartitionedScan(
    void *query_state, ThreadStateContainer *thread_states,
    const AggregationHashTable::ScanPartitionFn scan_fn) {
  // At this point, this aggregation table has a list of overflow partitions
  // that must be merged into a single aggregation hash table. For simplicity,
  // we create a new aggregation hash table per overflow partition, and merge
  // the contents of that partition into the new hash table. We use the HLL
  // estimates to size the hash table before construction so as to minimize the
  // growth factor. Each aggregation hash table partition will be built and
  // scanned in parallel.

  TPL_ASSERT(partition_heads_ != nullptr && merge_partition_fn_ != nullptr,
             "No overflow partitions allocated, or no merging function allocated. Did you call "
             "TransferMemoryAndPartitions() before issuing the partitioned scan?");

  // Determine the non-empty overflow partitions
  std::vector<uint32_t> nonempty_parts;
  nonempty_parts.reserve(kDefaultNumPartitions);
  for (uint32_t i = 0; i < kDefaultNumPartitions; i++) {
    if (partition_heads_[i] != nullptr) {
      nonempty_parts.push_back(i);
    }
  }

  util::Timer<std::milli> timer;
  timer.Start();

  tbb::parallel_for_each(nonempty_parts, [&](const uint32_t part_idx) {
    // Build a hash table over the given partition
    auto *agg_table_part = BuildTableOverPartition(query_state, part_idx);

    // Get a handle to the thread-local state of the executing thread
    auto *thread_state = thread_states->AccessCurrentThreadState();

    // Scan the partition
    scan_fn(query_state, thread_state, agg_table_part);
  });

  timer.Stop();

  const uint64_t tuple_count =
      std::accumulate(nonempty_parts.begin(), nonempty_parts.end(), uint64_t{0},
                      [&](const auto curr, const auto idx) {
                        return curr + partition_tables_[idx]->GetTupleCount();
                      });

  double tps = (tuple_count / timer.GetElapsed()) / 1000.0;
  LOG_INFO("Built and scanned {} tables totalling {} tuples in {:.2f} ms ({:.2f} mtps)",
           nonempty_parts.size(), tuple_count, timer.GetElapsed(), tps);
}

}  // namespace tpl::sql
