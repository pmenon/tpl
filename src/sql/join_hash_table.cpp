#include "sql/join_hash_table.h"

namespace tpl::sql {

JoinHashTable::JoinHashTable(util::Region *region, u32 tuple_size,
                             bool use_concise_ht) noexcept
    : entries_(region, sizeof(HashTableEntry) + tuple_size),
      num_elems_(0),
      built_(false),
      use_concise_ht_(use_concise_ht) {
  head_.next = nullptr;
}

void JoinHashTable::BuildGenericHashTable() {
  // TODO(pmenon): Use HLL++ sketches to better estimate size
  // TODO(pmenon): Use tagged insertions/probes if no bloom filter exists

  generic_hash_table_.SetSize(num_elems());

  for (HashTableEntry *entry = head_.next; entry != nullptr;) {
    HashTableEntry *next = entry->next;
    generic_hash_table_.Insert<false>(entry, entry->hash);
    entry = next;
  }
}

namespace {

// When using concise hash tables, we need to reorder the build-side tuples
// after they've been materialized. The amount of size of the reorder buffer we
// use (both the maximum size and the maximum number of elements, whichever
// comes first) is fixed in the constants below.

constexpr const u32 kBufferSizeInBytes = 16 * 1024;
constexpr const u32 kNumBufferElems = 4096;

class ReorderBuffer {
 public:
  ReorderBuffer(util::ChunkedVector &entries, u64 max_elems,
                u64 overflow_read_pos) noexcept
      : entry_size_(entries.element_size()),
        max_elems_(std::min(max_elems, kBufferSizeInBytes / entry_size_) - 1),
        end_read_idx_(overflow_read_pos),
        buffer_{},
        buffer_read_idx_(0),
        buffer_write_idx_(0),
        temp_buf_(buffer_ + (max_elems_ * entry_size_)),
        read_idx_(0),
        write_idx_(overflow_read_pos),
        entries_(entries) {
    TPL_ASSERT(max_elems > 0, "Maximum element count needs to be non-zero");
  }

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(ReorderBuffer);

  /// Fill a buffer's worth of data from the input entries buffer, if available
  void Fill() noexcept {
    byte *buffer_pos = buffer_;
    while (read_idx_ < end_read_idx_ && buffer_write_idx_ < max_elems_) {
      auto *entry = reinterpret_cast<HashTableEntry *>(entries_[read_idx_++]);

      // Skip processed entries
      if (entry->cht_slot.IsProcessed()) {
        continue;
      }

      if (!entry->cht_slot.IsOverflow()) {
        std::memcpy(buffer_pos, entry, entry_size_);
        entry->cht_slot.SetProcessed(true);

        buffer_pos += entry_size_;
        buffer_write_idx_++;
        continue;
      }

      // The entry is an overflow entry, try to find
      HashTableEntry *next_main_entry = nullptr;
      do {
        next_main_entry =
            reinterpret_cast<HashTableEntry *>(entries_[write_idx_++]);
      } while (next_main_entry->cht_slot.IsOverflow() ||
               next_main_entry->cht_slot.IsProcessed());

      // Copy the found entry into the buffer, then copy the overflow entry
      // into the empty slot. Make sure to mark the original entry as buffered
      std::memcpy(buffer_pos, next_main_entry, entry_size_);
      std::memcpy(next_main_entry, entry, entry_size_);

      entry->cht_slot.SetProcessed(true);

      buffer_pos += entry_size_;
      buffer_write_idx_++;
    }
  }

  void Reset() noexcept {
    buffer_read_idx_ = 0;
    buffer_write_idx_ = 0;
  }

  bool AtEnd() const noexcept { return read_idx_ == end_read_idx_; }

  byte *BufferBegin() noexcept { return buffer_; }

  byte *BufferEnd() noexcept {
    return buffer_ + (entry_size_ * buffer_write_idx_);
  }

  byte *temp_buffer() const noexcept { return temp_buf_; }

 private:
  // Size of entries
  const u64 entry_size_;
  // The maximum number of elements to buffer
  const u64 max_elems_;
  // The end index to stop reading from
  const u64 end_read_idx_;

  // The primary buffer space and the range of valid entries contained within
  byte buffer_[kBufferSizeInBytes];
  u64 buffer_read_idx_;
  u64 buffer_write_idx_;

  // A pointer to a temporary entry within the buffer space for worst-case
  // three-move swaps
  byte *const temp_buf_;

  // Indexes into the entries buffer
  u64 read_idx_;
  u64 write_idx_;
  util::ChunkedVector &entries_;
};

}  // namespace

void JoinHashTable::ReorderMainEntries() noexcept {
  const u64 elem_size = entries_.element_size();

  u64 overflow_idx = entries_.size() - concise_hash_table_.num_overflow();

  ReorderBuffer reorder_buf(entries_, kNumBufferElems, overflow_idx);
  HashTableEntry *RESTRICT targets[kNumBufferElems] = {nullptr};

  while (!reorder_buf.AtEnd()) {
    // Fill the reorder buffer with valid non-overflow entries
    reorder_buf.Fill();

    // Iterate over all to find target destinations
    for (auto [idx, buf_pos, buf_end] =
             std::tuple{0u, reorder_buf.BufferBegin(), reorder_buf.BufferEnd()};
         buf_pos != buf_end; buf_pos += elem_size, idx++) {
      auto *entry = reinterpret_cast<HashTableEntry *>(buf_pos);
      u64 dest_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot);
      targets[idx] = reinterpret_cast<HashTableEntry *>(entries_[dest_idx]);
    }

    for (auto [idx, write_pos, buf_pos, buf_end] =
             std::tuple{0u, reorder_buf.BufferBegin(),
                        reorder_buf.BufferBegin(), reorder_buf.BufferEnd()};
         buf_pos != buf_end; buf_pos += elem_size, idx++) {
      HashTableEntry *target = targets[idx];

      // If the target entry (i.e., the one we want to copy **into**) has
      // already been processed, then it's either in the reorder buffer or in
      // its final slot. In either case, we can perform a direct write.
      if (target->cht_slot.IsProcessed()) {
        std::memcpy(target, buf_pos, elem_size);
        target->cht_slot.SetProcessed(true);
        continue;
      }

      if (write_pos == buf_pos) {
        std::memcpy(reorder_buf.temp_buffer(), target, elem_size);
        std::memcpy(target, buf_pos, elem_size);
        std::memcpy(buf_pos, reorder_buf.temp_buffer(), elem_size);
        target->cht_slot.SetProcessed(true);
        write_pos += elem_size;
        continue;
      }

      std::memcpy(write_pos, target, elem_size);
      std::memcpy(target, buf_pos, elem_size);
      target->cht_slot.SetProcessed(true);
    }

    // Reset and try again
    reorder_buf.Reset();
  }
}

void JoinHashTable::ProcessOverflowEntries() noexcept {}

void JoinHashTable::BuildConciseHashTable() {
  // TODO(pmenon): Use HLL++ sketches to better estimate size

  concise_hash_table_.SetSize(num_elems());

  // Insert all entries into CHT
  for (HashTableEntry *entry = head_.next; entry != nullptr;) {
    HashTableEntry *next = entry->next;
    entry->cht_slot = concise_hash_table_.Insert(entry->hash);
    entry = next;
  }

  // Insertions complete, build it
  concise_hash_table_.Build();

  // Re-order main entries according to CHT order
  ReorderMainEntries();

  // Process overflow entries
  ProcessOverflowEntries();
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
    u32 num_tuples, hash_t hashes[], HashTableEntry *results[]) const {
  // TODO(pmenon): Use tagged insertions/probes if no bloom filter exists

  // Initial lookup
  for (u32 i = 0; i < num_tuples; i++) {
    results[i] = generic_hash_table_.FindChainHead(hashes[i]);
  }

  // Ensure find match on hash
  for (u32 i = 0; i < num_tuples; i++) {
    HashTableEntry *entry = results[i];
    while (entry != nullptr && entry->hash != hashes[i]) {
      entry = entry->next;
    }
    results[i] = entry;
  }
}

void JoinHashTable::LookupBatchInConciseHashTable(
    u32 num_tuples, hash_t hashes[], HashTableEntry *results[]) const {}

void JoinHashTable::LookupBatch(u32 num_tuples, hash_t hashes[],
                                HashTableEntry *results[]) const {
  TPL_ASSERT(is_built(), "Cannot perform lookup before table is built!");
  if (use_concise_hash_table()) {
    LookupBatchInConciseHashTable(num_tuples, hashes, results);
  } else {
    LookupBatchInGenericHashTable(num_tuples, hashes, results);
  }
}

}  // namespace tpl::sql
