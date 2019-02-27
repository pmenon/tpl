#include "sql/join_hash_table.h"

#include "logging/logger.h"
#include "util/memory.h"

namespace tpl::sql {

JoinHashTable::JoinHashTable(util::Region *region, u32 tuple_size,
                             bool use_concise_ht) noexcept
    : entries_(region, sizeof(HashTableEntry) + tuple_size),
      concise_hash_table_(0),
      num_elems_(0),
      built_(false),
      use_concise_ht_(use_concise_ht) {
  head_.next = nullptr;
}

void JoinHashTable::BuildGenericHashTable() {
  // TODO(pmenon): Use HLL++ sketches to better estimate size
  // TODO(pmenon): Use tagged insertions/probes if no bloom filter exists

  generic_hash_table_.SetSize(num_elems());

  for (u64 idx = 0, prefetch_idx = 16; idx < entries_.size();
       idx++, prefetch_idx++) {
    if (TPL_LIKELY(prefetch_idx < entries_.size())) {
      generic_hash_table_.PrefetchChainHead<false>(EntryAt(prefetch_idx)->hash);
    }

    HashTableEntry *entry = EntryAt(idx);
    generic_hash_table_.Insert<false>(entry, entry->hash);
  }
}

template <>
void JoinHashTable::InsertIntoConciseHashTableInternal<false>() noexcept {
  for (u64 idx = 0; idx < entries_.size(); idx++) {
    HashTableEntry *entry = EntryAt(idx);
    concise_hash_table_.Insert(entry, entry->hash);
  }
}

template <>
void JoinHashTable::InsertIntoConciseHashTableInternal<true>() noexcept {
  for (u64 idx = 0, prefetch_idx = 16; idx < entries_.size();
       idx++, prefetch_idx++) {
    if (TPL_LIKELY(prefetch_idx < entries_.size())) {
      concise_hash_table_.PrefetchSlotGroup<false>(EntryAt(prefetch_idx)->hash);
    }

    HashTableEntry *entry = EntryAt(idx);
    concise_hash_table_.Insert(entry, entry->hash);
  }
}

void JoinHashTable::InsertIntoConciseHashTable() noexcept {
  // Assume 8MB cache
  static constexpr const u32 kCacheSize = 1u << 23;

  if (concise_hash_table_.GetTotalMemoryUsage() > kCacheSize) {
    InsertIntoConciseHashTableInternal<true>();
  } else {
    InsertIntoConciseHashTableInternal<false>();
  }
}

namespace {

class ReorderBuffer {
 public:
  // Use a 16 KB internal buffer for temporary copies
  static constexpr const u32 kBufferSizeInBytes = 16 * 1024;

  ReorderBuffer(util::ChunkedVector &entries, u64 max_elems, u64 begin_read_idx,
                u64 end_read_idx) noexcept
      : entry_size_(entries.element_size()),
        max_elems_(std::min(max_elems, kBufferSizeInBytes / entry_size_) - 1),
        buffer_{},
        buffer_pos_(buffer_),
        temp_buf_(buffer_ + (max_elems_ * entry_size_)),
        read_idx_(begin_read_idx),
        end_read_idx_(end_read_idx),
        entries_(entries) {}

  DISALLOW_COPY_AND_MOVE(ReorderBuffer);

  bool Fill() noexcept {
    while (read_idx_ < end_read_idx_ && buffer_pos_ < temp_buf_) {
      auto *entry = reinterpret_cast<HashTableEntry *>(entries_[read_idx_++]);

      // Skip buffering processed entries
      if (entry->cht_slot.IsProcessed()) {
        continue;
      }

      // Copy and mark original entry as buffered
      std::memcpy(buffer_pos_, entry, entry_size_);
      entry->cht_slot.SetBuffered(true);

      // Bump pointer
      buffer_pos_ += entry_size_;
    }

    return buffer_pos_ > buffer_;
  }

  void Reset(byte *write_pos) noexcept { buffer_pos_ = write_pos; }

  byte *buffer_begin() noexcept { return buffer_; }

  byte *buffer_end() noexcept { return buffer_pos_; }

  byte *temp_buffer() const noexcept { return temp_buf_; }

 private:
  // Size of entries
  const u64 entry_size_;
  // The maximum number of elements to buffer
  const u64 max_elems_;

  // Buffer space for entries and a pointer into the space for the next entry
  byte buffer_[kBufferSizeInBytes];
  byte *buffer_pos_;

  // A pointer to the last entry slot in the buffer space; used for costly swaps
  byte *const temp_buf_;

  // The current and maximum index to read from in the entries list
  u64 read_idx_;
  const u64 end_read_idx_;
  util::ChunkedVector &entries_;
};

}  // namespace

void JoinHashTable::ReorderMainEntries() noexcept {
  const u64 elem_size = entries_.element_size();
  const u64 num_overflow_entries = concise_hash_table_.num_overflow();
  const u64 num_main_entries = entries_.size() - num_overflow_entries;
  u64 overflow_idx = num_main_entries;

  if (num_main_entries == 0) {
    return;
  }

  //
  // This function reorders the main entries in-place using an ephemeral reorder
  // buffer space. The general process is:
  // 1. Buffer N tuples. These can be either main or overflow entries.
  // 2. Find and store their destination addresses in the 'targets' buffer
  // 3. Try and store the buffered entry into the found (i.e., target)
  //    destination space. There are a few cases we handle:
  //    3a. If the target entry is 'PROCESSED', then the buffered entry is an
  //        overflow entry. We acquire an overflow slot and store the buffered
  //        entry there.
  //    3b. If the target entry is 'BUFFERED', it is either stored somewhere
  //        in the reorder buffer, or has been stored into its own target space.
  //        Either way, we can directly overwrite the target with the buffered
  //        entry and be done with it.
  //    3c. We need to swap the target and buffer entry. If the reorder buffer
  //        has room, copy the target into the buffer and write the buffered
  //        entry out.
  //    3d. If the reorder buffer has no room for this target entry, we use a
  //        temporary space to perform a (costly) three-way swap to buffer the
  //        target entry into the reorder buffer and write the buffered entry
  //        out. This should happen infrequently.
  // 4. Reset the reorder buffer and repeat.
  //

  HashTableEntry *targets[kDefaultVectorSize];
  ReorderBuffer reorder_buf(entries_, kDefaultVectorSize, 0, overflow_idx);

  while (reorder_buf.Fill()) {
    u64 idx = 0;
    for (auto buf_pos = reorder_buf.buffer_begin(),
              buf_end = reorder_buf.buffer_end();
         buf_pos != buf_end; buf_pos += elem_size, idx++) {
      HashTableEntry *entry = reinterpret_cast<HashTableEntry *>(buf_pos);
      u64 dest_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot);
      targets[idx] = EntryAt(dest_idx);
    }

    const u64 found = idx;
    u32 prefetch_idx = kPrefetchDistance;
    idx = 0;

    byte *buf_write_pos = reorder_buf.buffer_begin();
    for (auto buf_pos = reorder_buf.buffer_begin(),
              buf_end = reorder_buf.buffer_end();
         buf_pos != buf_end; buf_pos += elem_size, idx++) {
      HashTableEntry *dest = targets[idx];
      bool result = true;

      if (TPL_LIKELY(prefetch_idx < found)) {
        util::Prefetch<false, Locality::Medium>(targets[prefetch_idx++]);
      }

      if (dest->cht_slot.IsProcessed()) {
        dest = EntryAt(overflow_idx++);
        result = false;
      }

      if (dest->cht_slot.IsBuffered()) {
        std::memcpy(static_cast<void *>(dest), buf_pos, elem_size);
      } else if (buf_write_pos < buf_pos) {
        std::memcpy(buf_write_pos, static_cast<void *>(dest), elem_size);
        std::memcpy(static_cast<void *>(dest), buf_pos, elem_size);
        buf_write_pos += elem_size;
      } else {
        byte *const tmp = reorder_buf.temp_buffer();
        std::memcpy(tmp, static_cast<void *>(dest), elem_size);
        std::memcpy(static_cast<void *>(dest), buf_pos, elem_size);
        std::memcpy(buf_pos, tmp, elem_size);
        buf_write_pos += elem_size;
      }

      dest->cht_slot.SetProcessed(result);
    }

    reorder_buf.Reset(buf_write_pos);
  }
}

void JoinHashTable::ReorderOverflowEntries() noexcept {
  const u64 elem_size = entries_.element_size();
  const u64 num_overflow_entries = concise_hash_table_.num_overflow();
  const u64 num_main_entries = entries_.size() - num_overflow_entries;
  const u64 overflow_start_idx = num_main_entries;
  const u64 no_overflow = std::numeric_limits<u64>::max();

  if (num_overflow_entries == 0) {
    return;
  }

  //
  // This function reorders the overflow entries in-place. The high-level idea
  // is to reorder the entries stored in the overflow area so that probe chains
  // are stored contiguously.
  //

  HashTableEntry *parents[kDefaultVectorSize];

  for (u64 idx = 0; idx < num_main_entries; idx++) {
    EntryAt(idx)->overflow_count = 0;
  }

  for (u64 start = overflow_start_idx; start < entries_.size();
       start += kDefaultVectorSize) {
    u64 vec_size = std::min(u64{kDefaultVectorSize}, entries_.size() - start);
    u64 end = start + vec_size;

    for (u64 idx = start, write_idx = 0; idx < end; idx++, write_idx++) {
      HashTableEntry *entry = EntryAt(idx);
      u64 chain_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot);
      parents[write_idx] = EntryAt(chain_idx);
    }

    for (u64 idx = 0; idx < vec_size; idx++) {
      parents[idx]->overflow_count++;
    }
  }

  for (u64 idx = 0, count = 0; idx < num_main_entries; idx++) {
    HashTableEntry *entry = EntryAt(idx);
    count += entry->overflow_count;
    entry->overflow_count =
        (entry->overflow_count == 0 ? no_overflow : num_main_entries + count);
  }

  ReorderBuffer reorder_buf(entries_, kDefaultVectorSize, overflow_start_idx,
                            entries_.size());
  while (reorder_buf.Fill()) {
    u64 idx = 0;
    for (auto iter = reorder_buf.buffer_begin(), end = reorder_buf.buffer_end();
         iter != end; iter += elem_size, idx++) {
      HashTableEntry *entry = reinterpret_cast<HashTableEntry *>(iter);
      u64 dest_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot);
      parents[idx] = EntryAt(dest_idx);
    }

    idx = 0;

    byte *buf_write_pos = reorder_buf.buffer_begin();
    for (auto buf_pos = reorder_buf.buffer_begin(),
              buf_end = reorder_buf.buffer_end();
         buf_pos != buf_end; buf_pos += elem_size, idx++) {
      HashTableEntry *target = EntryAt(--parents[idx]->overflow_count);

      if (target->cht_slot.IsBuffered()) {
        std::memcpy(static_cast<void *>(target), buf_pos, elem_size);
      } else if (buf_write_pos < buf_pos) {
        std::memcpy(buf_write_pos, static_cast<void *>(target), elem_size);
        std::memcpy(static_cast<void *>(target), buf_pos, elem_size);
        buf_write_pos += elem_size;
      } else {
        byte *const tmp = reorder_buf.temp_buffer();
        std::memcpy(tmp, static_cast<void *>(target), elem_size);
        std::memcpy(static_cast<void *>(target), buf_pos, elem_size);
        std::memcpy(buf_write_pos, tmp, elem_size);
        buf_write_pos += elem_size;
      }

      target->cht_slot.SetProcessed(true);
    }

    reorder_buf.Reset(buf_write_pos);
  }

  for (u64 idx = 0; idx < num_main_entries; idx++) {
    HashTableEntry *entry = EntryAt(idx);
    entry->next =
        (entry->overflow_count == no_overflow ? nullptr
                                              : EntryAt(entry->overflow_count));
  }
}

void JoinHashTable::VerifyMainEntryOrder() noexcept {
#ifndef NDEBUG
  const u64 overflow_idx = entries_.size() - concise_hash_table_.num_overflow();
  for (u32 idx = 0; idx < overflow_idx; idx++) {
    auto *entry = reinterpret_cast<HashTableEntry *>(entries_[idx]);
    auto dest_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot);
    if (idx != dest_idx) {
      LOG_ERROR("Entry {} has CHT slot {}. Found @ {}, but should be @ {}",
                static_cast<void *>(entry), entry->cht_slot.GetSlotIndex(), idx,
                dest_idx);
    }
  }
#endif
}

void JoinHashTable::VerifyOverflowEntryOrder() noexcept {
#ifndef NDEBUG
#endif
}

void JoinHashTable::BuildConciseHashTable() {
  // TODO(pmenon): Use HLL++ sketches to better estimate size

  concise_hash_table_.SetSize(num_elems());

  InsertIntoConciseHashTable();

  concise_hash_table_.Build();

  LOG_DEBUG(
      "Concise Table Stats: {} entries, {} overflow ({} % overflow)",
      entries_.size(), concise_hash_table_.num_overflow(),
      100.0 * (concise_hash_table_.num_overflow() * 1.0 / entries_.size()));

  ReorderMainEntries();

  VerifyMainEntryOrder();

  ReorderOverflowEntries();

  VerifyOverflowEntryOrder();
}

void JoinHashTable::Build() {
  if (is_built()) {
    return;
  }

  if (use_concise_hash_table()) {
    BuildConciseHashTable();
  } else {
    BuildGenericHashTable();
  }

  built_ = true;
}

void JoinHashTable::LookupBatchInGenericHashTable(
    u32 num_tuples, const hash_t hashes[],
    const HashTableEntry *results[]) const {
  // TODO(pmenon): Use tagged insertions/probes if no bloom filter exists

  // Initial lookup
  for (u32 i = 0; i < num_tuples; i++) {
    results[i] = generic_hash_table_.FindChainHead(hashes[i]);
  }

  // Ensure find match on hash
  for (u32 i = 0; i < num_tuples; i++) {
    const HashTableEntry *entry = results[i];
    while (entry != nullptr && entry->hash != hashes[i]) {
      entry = entry->next;
    }
    results[i] = entry;
  }
}

void JoinHashTable::LookupBatchInConciseHashTable(
    UNUSED u32 num_tuples, UNUSED const hash_t hashes[],
    UNUSED const HashTableEntry *results[]) const {}

void JoinHashTable::LookupBatch(u32 num_tuples, const hash_t hashes[],
                                const HashTableEntry *results[]) const {
  TPL_ASSERT(is_built(), "Cannot perform lookup before table is built!");

  if (use_concise_hash_table()) {
    LookupBatchInConciseHashTable(num_tuples, hashes, results);
  } else {
    LookupBatchInGenericHashTable(num_tuples, hashes, results);
  }
}

}  // namespace tpl::sql
