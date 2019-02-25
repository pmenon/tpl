#include "sql/join_hash_table.h"

#include "logging/logger.h"

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
        buffer_pos_(buffer_),
        temp_buf_(buffer_ + (max_elems_ * entry_size_)),
        read_idx_(0),
        end_read_idx_(overflow_read_pos),
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
  byte *const RESTRICT temp_buf_;

  // The current and maximum index to read from in the entries list
  u64 read_idx_;
  const u64 end_read_idx_;
  util::ChunkedVector &entries_;
};

}  // namespace

void JoinHashTable::ReorderMainEntries() noexcept {
  const u64 elem_size = entries_.element_size();

  u64 overflow_idx = entries_.size() - concise_hash_table_.num_overflow();

  ReorderBuffer reorder_buf(entries_, kNumBufferElems, overflow_idx);
  HashTableEntry *RESTRICT targets[kNumBufferElems];

  while (reorder_buf.Fill()) {
    // First, find matches for buffered entries
    for (auto [idx, buf_pos, buf_end] = std::tuple(
             0u, reorder_buf.buffer_begin(), reorder_buf.buffer_end());
         buf_pos != buf_end; buf_pos += elem_size, idx++) {
      auto *entry = reinterpret_cast<HashTableEntry *>(buf_pos);
      u64 dest_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot);
      targets[idx] = reinterpret_cast<HashTableEntry *>(entries_[dest_idx]);
    }

    // Next, write buffered entries into their destinations
    byte *buf_write_pos = reorder_buf.buffer_begin();
    for (auto [idx, buf_read_pos, buf_end] = std::tuple(
             0u, reorder_buf.buffer_begin(), reorder_buf.buffer_end());
         buf_read_pos != buf_end; buf_read_pos += elem_size, idx++) {
      HashTableEntry *dest = targets[idx];

      // If the destination is 'PROCESSED', then the buffer entry is an overflow
      if (dest->cht_slot.IsProcessed()) {
        dest = reinterpret_cast<HashTableEntry *>(entries_[overflow_idx++]);
      }

      // If the destination is 'BUFFERED', a copy exists somewhere safe in the
      // reorder buffer. We can directly overwrite the destination's contents.
      if (dest->cht_slot.IsBuffered()) {
        std::memcpy(reinterpret_cast<byte *>(dest), buf_read_pos, elem_size);
        dest->cht_slot.SetProcessed(true);
        continue;
      }

      // The destination element has not been buffered; we need to bring it into
      // the reorder buffer before we can write the current entry into its
      // destination. If there is room in the buffer, copy it in and write the
      // current entry. Otherwise, perform a costly three-step swap using a
      // temporary buffer from the ROB.

      if (buf_write_pos < buf_read_pos) {
        std::memcpy(buf_write_pos, reinterpret_cast<byte *>(dest), elem_size);
        std::memcpy(reinterpret_cast<byte *>(dest), buf_read_pos, elem_size);
      } else {
        byte *const RESTRICT tmp = reorder_buf.temp_buffer();
        std::memcpy(tmp, reinterpret_cast<byte *>(dest), elem_size);
        std::memcpy(reinterpret_cast<byte *>(dest), buf_read_pos, elem_size);
        std::memcpy(buf_read_pos, tmp, elem_size);
      }

      dest->cht_slot.SetProcessed(true);
      buf_write_pos += elem_size;
    }

    // Reset and try again
    reorder_buf.Reset(buf_write_pos);
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

void JoinHashTable::ProcessOverflowEntries() noexcept {}

void JoinHashTable::BuildConciseHashTable() {
  // TODO(pmenon): Use HLL++ sketches to better estimate size

  concise_hash_table_.SetSize(num_elems());

  for (HashTableEntry *entry = head_.next; entry != nullptr;) {
    HashTableEntry *next = entry->next;
    entry->cht_slot = concise_hash_table_.Insert(entry->hash);
    entry = next;
  }

  concise_hash_table_.Build();

  LOG_DEBUG(
      "{} entries, {} overflow ({} % overflow)", entries_.size(),
      concise_hash_table_.num_overflow(),
      100.0 * (concise_hash_table_.num_overflow() * 1.0 / entries_.size()));

  ReorderMainEntries();

  VerifyMainEntryOrder();

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
