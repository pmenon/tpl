#pragma once

#include <atomic>

#include "util/common.h"
#include "util/macros.h"
#include "util/math_util.h"
#include "util/memory.h"

namespace tpl::runtime {

/// HashMap is a generic bytes-to-bytes hash table used for SQL processing. It
/// is implemented using a closed-addressing bucket-chained hash table with
/// pointer tagging.
///
/// This isn't general-purpose.
class HashTable {
 private:
  static const u32 kNumTagBits = 16;
  static const u32 kNumPointerBits = sizeof(u64) * 8 - kNumTagBits;
  static const i64 kMaskPointer = (~static_cast<u64>(0)) >> kNumTagBits;
  static const i64 kMaskTag = (~static_cast<u64>(0)) << kNumPointerBits;

 protected:
  // Generic struct representing an entry in the hash map
  struct EntryHeader {
    EntryHeader *next;
    hash_t hash;
  };

  // Given a hash value, return the head of the bucket chain ignoring any tag
  EntryHeader *FindChainHead(hash_t hash) const;

  // Given a hash value, return the head of the bucket chain removing the tag
  EntryHeader *FindChainHeadWithTag(hash_t hash) const;

 public:
  /// Constructor does not allocate memory. Callers must first call SetSize()
  /// before using this hash map.
  /// \param load_factor The desired load-factor for the table
  explicit HashTable(float load_factor = 0.7);

  /// Cleanup
  ~HashTable();

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(HashTable);

  /// Insert an entry into the hash table, ignoring tagging the pointer into the
  /// bucket head
  /// \tparam Concurrent Is the insert occurring concurrently with other inserts
  /// \param entry The entry to insert
  /// \param hash The hash value of the entry
  template <bool Concurrent = true>
  void Insert(EntryHeader *entry, hash_t hash);

  /// Insert an entry into the hash table, updating the tag in the bucket head
  /// \tparam Concurrent Is the insert occurring concurrently with other inserts
  /// \param entry The entry to insert
  /// \param hash The hash value of the entry
  template <bool Concurrent = true>
  void InsertTagged(EntryHeader *entry, hash_t hash);

  /// Explicitly set the size of the hash map
  /// \param new_size The new size represented as the number of elements to
  /// store in the map
  void SetSize(u64 new_size);

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  u64 num_elements() const { return num_elems_; }

  u64 capacity() const { return capacity_; }

  float load_factor() const { return load_factor_; }

 private:
  EntryHeader *UntagPointer(EntryHeader *entry) const;

  EntryHeader *UpdateTag(EntryHeader *old_entry, EntryHeader *new_entry) const;

  u64 TagHash(hash_t hash) const;

 private:
  // Main bucket table
  std::atomic<EntryHeader *> *entries_;

  // The mask to use to determine the bucket position of an entry given its hash
  u64 mask_;

  // The capacity of the table
  u64 capacity_;

  // The current number of elements stored in the table
  u64 num_elems_;

  // The current load-factory
  float load_factor_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline HashTable::HashTable(float load_factor)
    : entries_(nullptr),
      mask_(0),
      capacity_(0),
      num_elems_(0),
      load_factor_(load_factor) {}

inline HashTable::~HashTable() {
  if (entries_ != nullptr) {
    util::mem::FreeHugeArray(entries_, capacity_);
  }
}

inline HashTable::EntryHeader *HashTable::FindChainHead(hash_t hash) const {
  u64 pos = hash & mask_;
  return entries_[pos].load(std::memory_order_relaxed);
}

inline HashTable::EntryHeader *HashTable::FindChainHeadWithTag(
    hash_t hash) const {
  auto candidate = FindChainHead(hash);
  auto exists_in_chain = reinterpret_cast<intptr_t>(candidate) & TagHash(hash);
  return (exists_in_chain ? UntagPointer(candidate) : nullptr);
}

template <bool Concurrent>
inline void HashTable::Insert(HashTable::EntryHeader *entry, hash_t hash) {
  const auto pos = hash & mask_;

  TPL_ASSERT(pos < capacity_, "Computed table position exceeds capacity!");
  TPL_ASSERT(entry->hash == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    std::atomic<EntryHeader *> &loc = entries_[pos];
    EntryHeader *old_entry = loc.load();
    do {
      entry->next = old_entry;
    } while (!loc.compare_exchange_weak(old_entry, entry));
  } else {
    std::atomic<EntryHeader *> &loc = entries_[pos];
    EntryHeader *old_entry = loc.load(std::memory_order_relaxed);
    entry->next = old_entry;
    loc.store(entry, std::memory_order_relaxed);
  }

  num_elems_++;
}

template <bool Concurrent>
inline void HashTable::InsertTagged(HashTable::EntryHeader *entry,
                                    hash_t hash) {
  const auto pos = hash & mask_;

  TPL_ASSERT(pos < capacity_, "Computed table position exceeds capacity!");
  TPL_ASSERT(entry->hash == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    std::atomic<EntryHeader *> &loc = entries_[pos];
    EntryHeader *old_entry = loc.load();
    EntryHeader *new_entry;
    do {
      entry->next = UntagPointer(old_entry);
      new_entry = UpdateTag(old_entry, entry);
    } while (!loc.compare_exchange_weak(old_entry, new_entry));

  } else {
    std::atomic<EntryHeader *> &loc = entries_[pos];
    EntryHeader *old_entry = loc.load(std::memory_order_relaxed);
    entry->next = UntagPointer(old_entry);
    loc.store(UpdateTag(old_entry, entry), std::memory_order_relaxed);
  }

  num_elems_++;
}

inline void HashTable::SetSize(u64 new_size) {
  TPL_ASSERT(new_size > 0, "New size cannot be zero!");
  if (entries_ != nullptr) {
    util::mem::FreeHuge(entries_,
                        capacity() * sizeof(std::atomic<EntryHeader *>));
  }

  u64 next_size = util::MathUtil::NextPowerOf2(new_size);
  if (next_size < new_size / load_factor()) {
    next_size *= 2;
  }

  capacity_ = next_size;
  mask_ = capacity_ - 1;
  entries_ = util::mem::MallocHugeArray<std::atomic<EntryHeader *>>(capacity_);
}

inline HashTable::EntryHeader *HashTable::UntagPointer(
    HashTable::EntryHeader *entry) const {
  auto ptr = reinterpret_cast<intptr_t>(entry);
  return reinterpret_cast<EntryHeader *>(ptr & kMaskPointer);
}

inline HashTable::EntryHeader *HashTable::UpdateTag(
    HashTable::EntryHeader *old_entry,
    HashTable::EntryHeader *new_entry) const {
  auto old_ptr = reinterpret_cast<intptr_t>(old_entry);
  auto ptr = reinterpret_cast<intptr_t>(new_entry);
  return reinterpret_cast<EntryHeader *>(ptr | old_ptr |
                                         TagHash(new_entry->hash));
}

inline u64 HashTable::TagHash(hash_t hash) const {
  auto tag_bit_pos = hash >> (sizeof(u64) * 8 - 4);
  TPL_ASSERT(tag_bit_pos < kNumTagBits, "Invalid tag!");
  return static_cast<u64>(1) << (tag_bit_pos + kNumPointerBits);
}

}  // namespace tpl::runtime