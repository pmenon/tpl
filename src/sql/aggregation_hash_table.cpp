#include "sql/aggregation_hash_table.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "count/hll.h"

#include "tbb/parallel_for_each.h"
#include "tbb/task_scheduler_init.h"

#include "common/cpu_info.h"
#include "common/exception.h"
#include "logging/logger.h"
#include "sql/constant_vector.h"
#include "sql/generic_value.h"
#include "sql/thread_state_container.h"
#include "sql/vector_operations/vector_operators.h"
#include "sql/vector_projection_iterator.h"
#include "util/bit_util.h"
#include "util/math_util.h"
#include "util/timer.h"

namespace tpl::sql {

class AggregationHashTable::HashToGroupIdMap {
  // Marker indicating an empty slot in the hash table
  static constexpr const uint16_t kEmpty = std::numeric_limits<uint16_t>::max();

 public:
  // An entry in the hash table.
  struct Entry {
    uint32_t hash;
    uint16_t gid;
    uint16_t next;
  };

  HashToGroupIdMap() {
    const uint64_t max_size = kDefaultVectorSize;
    capacity_ = max_size * 2;
    mask_ = capacity_ - 1;
    entries_ = std::unique_ptr<uint16_t[]>(new uint16_t[capacity_]{kEmpty});
    storage_ = std::make_unique<Entry[]>(max_size);
    storage_used_ = 0;
  }

  // This class cannot be copied or moved.
  DISALLOW_COPY_AND_MOVE(HashToGroupIdMap);

  // Remove all elements from the hash table.
  void Clear() {
    storage_used_ = 0;
    std::memset(entries_.get(), kEmpty, capacity_ * sizeof(uint16_t));
  }

  // Find the group associated to the input hash, but only if the predicate is
  // true. If no such value is found, return a nullptr.
  template <typename P>
  uint16_t *Find(const hash_t hash, P p) {
    uint16_t candidate = entries_[hash & mask_];
    if (candidate == kEmpty) {
      return nullptr;
    }
    for (auto candidate_ptr = &storage_[candidate]; candidate != kEmpty;
         candidate = candidate_ptr->next, candidate_ptr = &storage_[candidate]) {
      if (p(candidate_ptr->gid)) {
        return &candidate_ptr->gid;
      }
    }
    return nullptr;
  }

  // Insert a new hash-group mapping
  void Insert(const hash_t hash, const uint16_t gid) {
    TPL_ASSERT(storage_used_ < kDefaultVectorSize, "Too many elements in table");

    // Determine the spot in the storage the new entry occupies
    uint16_t entry_pos = storage_used_++;
    Entry *entry = &storage_[entry_pos];

    // Put the new entry at the head of the chain
    entry->next = entries_[hash & mask_];
    entries_[hash & mask_] = entry_pos;

    // Fill the entry
    entry->hash = hash;
    entry->gid = gid;
  }

  // Iterators
  Entry *begin() { return storage_.get(); }
  Entry *end() { return storage_.get() + storage_used_; }

 private:
  // The mask to use to map hashes to entry/directory slots
  hash_t mask_;
  // The main directory mapping to indexes of storage entries
  std::unique_ptr<uint16_t[]> entries_;
  // Main array storage of hash table data (keys, values, hashes, etc.)
  std::unique_ptr<Entry[]> storage_;
  // The capacity of the directory
  uint16_t capacity_;
  // The number of slots of storage that have been used
  uint16_t storage_used_;
};

// ---------------------------------------------------------
// Batch Process State
// ---------------------------------------------------------

AggregationHashTable::BatchProcessState::BatchProcessState(
    std::unique_ptr<libcount::HLL> estimator, std::unique_ptr<HashToGroupIdMap> hash_to_group_map)
    : hll_estimator(std::move(estimator)),
      hash_to_group_map(std::move(hash_to_group_map)),
      groups_not_found(kDefaultVectorSize),
      groups_found(kDefaultVectorSize),
      key_not_equal(kDefaultVectorSize),
      key_equal(kDefaultVectorSize) {
  hash_and_entries.Initialize({TypeId::Hash, TypeId::Pointer});
}

AggregationHashTable::BatchProcessState::~BatchProcessState() = default;

void AggregationHashTable::BatchProcessState::Reset(VectorProjectionIterator *input_batch) {
  // Resize the lists if they don't match the input. This should only happen
  // once, on the last input batch where the size may be less than one full
  // vector.
  const auto count = input_batch->GetTotalTupleCount();
  if (count != hash_and_entries.GetTotalTupleCount()) {
    hash_and_entries.Reset(count);
    groups_not_found.Resize(count);
    groups_found.Resize(count);
    key_not_equal.Resize(count);
    key_equal.Resize(count);
  }

  // Initially, there are no groups found and all keys are unequal
  input_batch->GetVectorProjection()->CopySelectionsTo(&groups_not_found);
  input_batch->GetVectorProjection()->CopySelectionsTo(&key_not_equal);

  // Clear the rest of the lists
  groups_found.Clear();
  key_equal.Clear();

  // Clear the collision table
  hash_to_group_map->Clear();
}

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
    // The estimates array uses HLL instances acquired from libcount. We own
    // them so we have to delete them manually.
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

HashTableEntry *AggregationHashTable::AllocateEntryInternal(const hash_t hash) {
  // Allocate an entry
  auto *entry = reinterpret_cast<HashTableEntry *>(entries_.append());
  entry->hash = hash;
  entry->next = nullptr;

  // Insert into table
  hash_table_.Insert<false>(entry);

  // Done
  return entry;
}

byte *AggregationHashTable::AllocInputTuple(const hash_t hash) {
  // Grow if need be
  if (NeedsToGrow()) {
    Grow();
  }

  // Allocate an entry
  HashTableEntry *entry = AllocateEntryInternal(hash);

  // Return the payload so the client can write into it
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
  if (NeedsToFlushToOverflowPartitions()) {
    FlushToOverflowPartitions();
  }
  return ret;
}

void AggregationHashTable::ComputeHash(VectorProjectionIterator *input_batch,
                                       const std::vector<uint32_t> &key_indexes) {
  auto *vector_projection = input_batch->GetVectorProjection();
  vector_projection->Hash(key_indexes, batch_state_->Hashes());
}

void AggregationHashTable::LookupInitial() {
  // For every active hash value in the hashes vector, perform a lookup in the
  // hash table and store the head of the bucket chain into the entries vector.
  auto *RESTRICT raw_entries =
      reinterpret_cast<const HashTableEntry **>(batch_state_->Entries()->GetData());
  VectorOps::ExecTyped<hash_t>(*batch_state_->Hashes(),
                               [&](const hash_t hash_val, const uint64_t i, const uint64_t k) {
                                 raw_entries[i] = hash_table_.FindChainHead(hash_val);
                               });

  // Tuples that did not find a matching group will have null HashTableEntry
  // pointers in the entries vector. Find and store their TIDs in the
  // groups-not-found list.
  ConstantVector null_ptr(GenericValue::CreatePointer(0));
  VectorOps::SelectEqual(*batch_state_->Entries(), null_ptr, batch_state_->GroupsNotFound());

  // Tuples that DID find a group still need to check for matching keys. Collect
  // their TIDs in the key-not-equal list.
  batch_state_->KeyNotEqual()->UnsetFrom(*batch_state_->GroupsNotFound());
}

namespace {

template <typename T>
void TemplatedCompareKey(const Vector &probe_keys, const Vector &entries,
                         const std::size_t key_offset, TupleIdList *key_equal_tids) {
  auto *RESTRICT raw_probe_keys = reinterpret_cast<const T *>(probe_keys.GetData());
  auto *RESTRICT raw_entries = reinterpret_cast<const HashTableEntry **>(entries.GetData());
  key_equal_tids->Filter([&](uint64_t i) {
    auto *RESTRICT table_key = reinterpret_cast<const T *>(raw_entries[i]->payload + key_offset);
    return raw_probe_keys[i] == *table_key;
  });
}

void CompareKey(const Vector &probe_keys, const Vector &entries, const std::size_t key_offset,
                TupleIdList *key_equal_tids) {
  switch (probe_keys.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedCompareKey<bool>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::TinyInt:
      TemplatedCompareKey<int8_t>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::SmallInt:
      TemplatedCompareKey<int16_t>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Integer:
      TemplatedCompareKey<int32_t>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::BigInt:
      TemplatedCompareKey<int64_t>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Float:
      TemplatedCompareKey<float>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Double:
      TemplatedCompareKey<double>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Date:
      TemplatedCompareKey<Date>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Varchar:
      TemplatedCompareKey<VarlenEntry>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Varbinary:
      TemplatedCompareKey<Blob>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    default:
      throw NotImplementedException("key comparison on type {} not supported",
                                    TypeIdToString(probe_keys.GetTypeId()));
  }
}

}  // namespace

void AggregationHashTable::CheckKeyEquality(VectorProjectionIterator *input_batch,
                                            const std::vector<uint32_t> &key_indexes) {
  // The list of tuples whose keys need to be checked is stored in
  // key-not-equal. We copy it to the key-equal list which we'll use as the
  // running list of tuples that DO have matching keys to table aggregates.
  batch_state_->KeyEqual()->AssignFrom(*batch_state_->KeyNotEqual());

  // If no tuples to check, we can exit.
  if (batch_state_->KeyEqual()->IsEmpty()) {
    return;
  }

  // Check all key components one at a time.
  std::size_t key_offset = 0;
  for (const auto key_index : key_indexes) {
    const Vector *key_vector = input_batch->GetVectorProjection()->GetColumn(key_index);
    CompareKey(*key_vector, *batch_state_->Entries(), key_offset, batch_state_->KeyEqual());
    key_offset += GetTypeIdSize(key_vector->GetTypeId());
  }

  // The key-equal list now contains the TIDs of all tuples that succeeded in
  // matching keys with their associated group. Add them to the running list
  // of groups-found.
  batch_state_->GroupsFound()->UnionWith(*batch_state_->KeyEqual());

  // Tuples that didn't match keys need to proceed through the chain of
  // candidate entries. Collect them in the key-not-equal list.
  batch_state_->KeyNotEqual()->UnsetFrom(*batch_state_->KeyEqual());
}

void AggregationHashTable::FollowNext() {
  auto *raw_entries = reinterpret_cast<HashTableEntry **>(batch_state_->Entries()->GetData());
  batch_state_->KeyNotEqual()->Filter(
      [&](uint64_t i) { return (raw_entries[i] = raw_entries[i]->next) != nullptr; });
}

void AggregationHashTable::FindGroups(VectorProjectionIterator *input_batch,
                                      const std::vector<uint32_t> &key_indexes) {
  // Perform initial lookup.
  LookupInitial();

  // Check keys.
  CheckKeyEquality(input_batch, key_indexes);

  // While we have unmatched keys, move along chain.
  while (!batch_state_->KeyNotEqual()->IsEmpty()) {
    FollowNext();
    CheckKeyEquality(input_batch, key_indexes);
  }
}

namespace {

template <typename T, typename F>
void TemplatedFixGrouping(AggregationHashTable::HashToGroupIdMap *hash_to_group_map,
                          const Vector &hashes, const Vector &entries, const Vector &probe_keys,
                          TupleIdList *tid_list, F f) {
  auto *RESTRICT raw_hashes = reinterpret_cast<const hash_t *>(hashes.GetData());
  auto *RESTRICT raw_entries = reinterpret_cast<const HashTableEntry **>(entries.GetData());
  auto *RESTRICT raw_keys = reinterpret_cast<const T *>(probe_keys.GetData());
  tid_list->Filter([&](const uint64_t i) {
    auto *gid = hash_to_group_map->Find(
        raw_hashes[i], [&](const uint16_t gid) { return raw_keys[gid] == raw_keys[i]; });
    if (gid != nullptr) {
      raw_entries[i] = raw_entries[*gid];
    } else {
      hash_to_group_map->Insert(raw_hashes[i], i);
      raw_entries[i] = f(raw_hashes[i]);
    }
    return gid != nullptr;
  });
}

template <typename F>
void FixGrouping(AggregationHashTable::HashToGroupIdMap *groups, const Vector &hashes,
                 const Vector &entries, const Vector &probe_keys, TupleIdList *tid_list, F f) {
  switch (probe_keys.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedFixGrouping<bool, F>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::TinyInt:
      TemplatedFixGrouping<int8_t, F>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::SmallInt:
      TemplatedFixGrouping<int16_t, F>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Integer:
      TemplatedFixGrouping<int32_t, F>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::BigInt:
      TemplatedFixGrouping<int64_t, F>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Float:
      TemplatedFixGrouping<float, F>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Double:
      TemplatedFixGrouping<double, F>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Date:
      TemplatedFixGrouping<Date, F>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Varchar:
      TemplatedFixGrouping<VarlenEntry, F>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Varbinary:
      TemplatedFixGrouping<Blob, F>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    default:
      throw NotImplementedException("key comparison on type {} not supported",
                                    TypeIdToString(probe_keys.GetTypeId()));
  }
}

}  // namespace

void AggregationHashTable::CreateMissingGroups(
    VectorProjectionIterator *input_batch, const std::vector<uint32_t> &key_indexes,
    const AggregationHashTable::VectorInitAggFn init_agg_fn) {
  // The groups-found list contains all tuples that found a matching group in
  // the aggregation hash table. Thus, the list of tuples that did not find a
  // match is the complement of the groups-found list.
  batch_state_->GroupsNotFound()->UnsetFrom(*batch_state_->GroupsFound());

  // If all tuples found a matching group, we don't need to create any.
  if (batch_state_->GroupsNotFound()->IsEmpty()) {
    return;
  }

  // Find and resolve duplicate keys in this batch.
  batch_state_->KeyNotEqual()->AssignFrom(*batch_state_->GroupsNotFound());
  batch_state_->KeyEqual()->AssignFrom(*batch_state_->GroupsNotFound());
  for (const auto key_index : key_indexes) {
    const Vector *key_vector = input_batch->GetVectorProjection()->GetColumn(key_index);
    FixGrouping(batch_state_->HashToGroupMap(),  // Hash-to-group mapping
                *batch_state_->Hashes(),         // Hashes
                *batch_state_->Entries(),        // Entries
                *key_vector,                     // Keys
                batch_state_->KeyEqual(),        // The running list of tuples that found a match
                [this](const hash_t hash) { return AllocateEntryInternal(hash); });
  }

  // The key-not-equal list contains the list of all TIDs that did not find a
  // matching group. The key-equal list contains TIDs tuples that found a
  // matching group WITHIN this batch. The difference between these lists is
  // the list of tuples that require NEW groups.
  batch_state_->KeyNotEqual()->UnsetFrom(*batch_state_->KeyEqual());

  // Let the initialization function handle all the newly created aggregates.
  VectorProjectionIterator iter(batch_state_->Projection(), batch_state_->KeyNotEqual());
  input_batch->SetVectorProjection(input_batch->GetVectorProjection(), batch_state_->KeyNotEqual());
  init_agg_fn(&iter, input_batch);

  // Finish up
  batch_state_->GroupsFound()->UnionWith(*batch_state_->KeyNotEqual());
  batch_state_->GroupsFound()->UnionWith(*batch_state_->GroupsNotFound());
}

void AggregationHashTable::AdvanceGroups(
    VectorProjectionIterator *input_batch,
    const AggregationHashTable::VectorAdvanceAggFn advance_agg_fn) {
  // The list of tuples that found a match is stored in the groups-found list.
  // We let the callback function iterate over only these tuples and update
  // the aggregates as need be.
  VectorProjectionIterator iter(batch_state_->Projection(), batch_state_->GroupsFound());
  input_batch->SetVectorProjection(input_batch->GetVectorProjection(), batch_state_->GroupsFound());
  advance_agg_fn(&iter, input_batch);
}

void AggregationHashTable::ProcessBatch(
    VectorProjectionIterator *input_batch, const std::vector<uint32_t> &key_indexes,
    const AggregationHashTable::VectorInitAggFn init_agg_fn,
    const AggregationHashTable::VectorAdvanceAggFn advance_agg_fn,
    const bool partitioned_aggregation) {
  // No-op if the iterator is empty.
  if (input_batch->IsEmpty()) {
    return;
  }

  // Initialize the batch state if need be. Note: this is only performed once.
  if (TPL_UNLIKELY(batch_state_ == nullptr)) {
    batch_state_ = memory_->MakeObject<BatchProcessState>(
        libcount::HLL::Create(kDefaultHLLPrecision),  // The Hyper-Log-Log estimator
        std::make_unique<HashToGroupIdMap>());        // The Hash-to-GroupID map
  }

  // Reset state for the incoming batch.
  batch_state_->Reset(input_batch);

  // Compute the hashes.
  ComputeHash(input_batch, key_indexes);

  // Find groups.
  FindGroups(input_batch, key_indexes);

  // Creating missing groups.
  CreateMissingGroups(input_batch, key_indexes, init_agg_fn);

  // If the caller requested a partitioned aggregation, drain the main hash
  // table out to the overflow partitions, but only if needed.
  if (partitioned_aggregation) {
    if (NeedsToFlushToOverflowPartitions()) {
      FlushToOverflowPartitions();
    }
  } else {
    if (NeedsToGrow()) {
      Grow();
    }
  }

  // Advance the aggregates for all tuples that found a match.
  AdvanceGroups(input_batch, advance_agg_fn);
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

AggregationHashTable *AggregationHashTable::GetOrBuildTableOverPartition(
    void *query_state, const uint32_t partition_idx) {
  TPL_ASSERT(partition_idx < kDefaultNumPartitions, "Out-of-bounds partition access");
  TPL_ASSERT(partition_heads_[partition_idx] != nullptr,
             "Should not build aggregation table over empty partition!");
  TPL_ASSERT(
      merge_partition_fn_ != nullptr,
      "Merging function was not provided! Did you forget to call TransferMemoryAndPartitions()?");

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
    auto agg_table_partition = GetOrBuildTableOverPartition(query_state, part_idx);

    // Get a handle to the thread-local state of the executing thread
    auto thread_state = thread_states->AccessCurrentThreadState();

    // Scan the partition
    scan_fn(query_state, thread_state, agg_table_partition);
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

void AggregationHashTable::BuildAllPartitions(void *query_state) {
  TPL_ASSERT(partition_tables_ == nullptr, "Should not have built aggregation hash tables already");
  partition_tables_ = memory_->AllocateArray<AggregationHashTable *>(kDefaultNumPartitions, true);

  // Find non-empty partitions.
  llvm::SmallVector<uint32_t, kDefaultNumPartitions> nonempty_parts;
  for (uint32_t part_idx = 0; part_idx < kDefaultNumPartitions; part_idx++) {
    if (partition_heads_[part_idx] != nullptr) {
      nonempty_parts.push_back(part_idx);
    }
  }

  // For each valid partition, build a hash table over its contents.
  tbb::parallel_for_each(nonempty_parts, [&](const uint32_t part_idx) {
    GetOrBuildTableOverPartition(query_state, part_idx);
  });
}

void AggregationHashTable::Repartition() {
  // Find all non-empty partitions.
  llvm::SmallVector<AggregationHashTable *, kDefaultNumPartitions> nonempty_tables;
  for (uint32_t part_idx = 0; part_idx < kDefaultNumPartitions; part_idx++) {
    if (partition_tables_[part_idx] != nullptr) {
      nonempty_tables.push_back(partition_tables_[part_idx]);
    }
  }

  // First, flush all hash table partitions to their own overflow buckets.
  tbb::parallel_for_each(nonempty_tables, [&](auto table) { table->FlushToOverflowPartitions(); });

  // Now, transfer each hash table partition's overflow buckets to us.
  for (auto *table : nonempty_tables) {
    for (uint32_t part_idx = 0; part_idx < kDefaultNumPartitions; part_idx++) {
      if (table->partition_heads_[part_idx] != nullptr) {
        table->partition_tails_[part_idx]->next = partition_heads_[part_idx];
        partition_heads_[part_idx] = table->partition_heads_[part_idx];
        if (partition_tails_[part_idx] == nullptr) {
          partition_tails_[part_idx] = table->partition_tails_[part_idx];
        }
      }
    }

    // Move partitioned hash table memory into this main hash table.
    owned_entries_.emplace_back(std::move(table->entries_));
  }
}

void AggregationHashTable::MergePartitions(AggregationHashTable *target, void *query_state,
                                           AggregationHashTable::MergePartitionFn merge_func) {
  if (target->partition_tables_ == nullptr) {
    target->partition_tables_ =
        memory_->AllocateArray<AggregationHashTable *>(kDefaultNumPartitions, true);
  }

  // Find non-empty partitions.
  llvm::SmallVector<uint32_t, kDefaultNumPartitions> nonempty_parts;
  for (uint32_t part_idx = 0; part_idx < kDefaultNumPartitions; part_idx++) {
    if (partition_heads_[part_idx] != nullptr) {
      nonempty_parts.push_back(part_idx);
    }
  }

  // Merge overflow data into the appropriate partitioned table in the target.
  tbb::parallel_for_each(nonempty_parts, [&](const uint32_t part_idx) {
    // Get the partitioned hash table from the target.
    auto agg_table_partition = target->GetOrBuildTableOverPartition(query_state, part_idx);

    // Merge our overflow partition into target table.
    AHTOverflowPartitionIterator iter(partition_heads_ + part_idx, partition_heads_ + part_idx + 1);
    merge_func(query_state, agg_table_partition, &iter);
  });

  // Move our memory to the target.
  target->owned_entries_.emplace_back(std::move(entries_));
}

}  // namespace tpl::sql
