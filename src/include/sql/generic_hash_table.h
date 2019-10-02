#pragma once

#include <atomic>
#include <utility>

#include "common/common.h"
#include "common/macros.h"
#include "common/memory.h"
#include "sql/hash_table_entry.h"
#include "sql/memory_pool.h"

namespace tpl::sql {

/**
 * GenericHashTable serves as a dead-simple hash table for joins and aggregations in TPL. It is a
 * generic bytes-to-bytes hash table implemented as a bucket-chained table with pointer tagging.
 * Pointer tagging uses the first @em GenericHashTable::kNumTagBits bits of the entry pointers in
 * the main bucket directory as a bloom filter. It optionally supports concurrent inserts (and
 * trivially concurrent probes). This class only stores pointers into externally managed storage,
 * it does not store any hash table data internally at all.
 *
 * Note that this class makes use of the @em HashTableEntry::next pointer to implement the linked
 * list bucket chain.
 */
class GenericHashTable {
 private:
  static constexpr const uint32_t kNumTagBits = 16;
  static constexpr const uint32_t kNumPointerBits = sizeof(uint8_t *) * 8 - kNumTagBits;
  static constexpr const uint64_t kMaskPointer = (~0ull) >> kNumTagBits;
  static constexpr const uint64_t kMaskTag = (~0ull) << kNumPointerBits;

 public:
  /**
   * Create an empty hash table. Callers must first call SetSize() before using this hash table.
   * @param load_factor The desired load-factor for the table
   */
  explicit GenericHashTable(float load_factor = 0.7) noexcept;

  /**
   * Cleanup.
   */
  ~GenericHashTable();

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(GenericHashTable);

  /**
   * Insert an entry into the hash table, ignoring tagging the pointer into the bucket head.
   * @tparam Concurrent Is the insert occurring concurrently with other inserts
   * @param new_entry The entry to insert
   * @param hash The hash value of the entry
   */
  template <bool Concurrent>
  void Insert(HashTableEntry *new_entry, hash_t hash);

  /**
   * Insert an entry into the hash table, updating the tag in the bucket head
   * @tparam Concurrent Is the insert occurring concurrently with other inserts
   * @param new_entry The entry to insert
   * @param hash The hash value of the entry
   */
  template <bool Concurrent>
  void InsertTagged(HashTableEntry *new_entry, hash_t hash);

  /**
   * Explicitly set the size of the hash table to support at least @em new_size elements with good
   * performance.
   * @param new_size The expected number of elements to size the table for
   */
  void SetSize(uint64_t new_size);

  /**
   * Prefetch the head of the bucket chain for the hash @em hash.
   * @tparam ForRead Whether the prefetch is intended for a subsequent read op
   * @param hash The hash value of the element to prefetch
   */
  template <bool ForRead>
  void PrefetchChainHead(hash_t hash) const;

  /**
   * Given a hash value, return the head of the bucket chain ignoring any tag. This probe is
   * performed assuming no concurrent access into the table.
   * @param hash The hash value of the element to find
   * @return The (potentially null) head of the bucket chain for the given hash
   */
  HashTableEntry *FindChainHead(hash_t hash) const;

  /**
   * Given a hash value, return the head of the bucket chain removing the tag. This probe is
   * performed assuming no concurrent access into the table.
   * @param hash The hash value of the element to find
   * @return The (potentially null) head of the bucket chain for the given hash
   */
  HashTableEntry *FindChainHeadWithTag(hash_t hash) const;

  /**
   * Empty all entries in this hash table into the sink functor. After this function exits, the hash
   * table is empty.
   * @tparam F The function must be of the form void(*)(HashTableEntry*)
   * @param sink The sink of all entries in the hash table
   */
  template <typename F>
  void FlushEntries(const F &sink);

  /**
   * @return The total number of bytes this hash table has allocated.
   */
  uint64_t GetTotalMemoryUsage() const { return sizeof(HashTableEntry *) * GetCapacity(); }

  /**
   * @return The number of elements stored in this hash table.
   */
  uint64_t GetElementCount() const { return num_elems_.load(std::memory_order_relaxed); }

  /**
   * @return The maximum number of elements this hash table can store at its current size.
   */
  uint64_t GetCapacity() const { return capacity_; }

  /**
   * @return The configured load factor for the table's directory. Note that this isn't the load
   *         factor value is normally thought of: # elems / # slots. Since this is a bucket-chained
   *         table, load factors can exceed 1.0 if chains are long.
   */
  float GetLoadFactor() const { return load_factor_; }

 private:
  template <bool UseTag>
  friend class GenericHashTableIterator;
  template <bool UseTag>
  friend class GenericHashTableVectorIterator;

  // -------------------------------------------------------
  // Tag-related operations
  // -------------------------------------------------------

  // Given a tagged HashTableEntry pointer, strip out the tag bits and return an untagged
  // HashTableEntry pointer
  static HashTableEntry *UntagPointer(const HashTableEntry *const entry) {
    auto ptr = reinterpret_cast<uintptr_t>(entry);
    return reinterpret_cast<HashTableEntry *>(ptr & kMaskPointer);
  }

  static HashTableEntry *UpdateTag(const HashTableEntry *const tagged_old_entry,
                                   const HashTableEntry *const untagged_new_entry) {
    auto old_tagged_ptr = reinterpret_cast<uintptr_t>(tagged_old_entry);
    auto new_untagged_ptr = reinterpret_cast<uintptr_t>(untagged_new_entry);
    auto new_tagged_ptr = (new_untagged_ptr & kMaskPointer) | (old_tagged_ptr & kMaskTag) |
                          TagHash(untagged_new_entry->hash);
    return reinterpret_cast<HashTableEntry *>(new_tagged_ptr);
  }

  static uint64_t TagHash(const hash_t hash) {
    // We use the given hash value to obtain a bit position in the tag to set. We need to extract a
    // signature from the hash value in the range [0, kNumTagBits), so we take the log2(kNumTagBits)
    // most significant bits to determine which bit in the tag to set.
    auto tag_bit_pos = hash >> (sizeof(hash_t) * 8 - 4);
    TPL_ASSERT(tag_bit_pos < kNumTagBits, "Invalid tag!");
    return 1ull << (tag_bit_pos + kNumPointerBits);
  }

 private:
  // Main bucket table
  std::atomic<HashTableEntry *> *entries_;

  // The mask to use to determine the bucket position of an entry given its hash
  uint64_t mask_;

  // The capacity of the directory
  uint64_t capacity_;

  // The current number of elements stored in the table. Atomic because it **MAY** be modified
  // concurrently during insertions.
  std::atomic<uint64_t> num_elems_;

  // The current load-factor
  float load_factor_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

template <bool ForRead>
void GenericHashTable::PrefetchChainHead(hash_t hash) const {
  const uint64_t pos = hash & mask_;
  Memory::Prefetch<ForRead, Locality::Low>(entries_ + pos);
}

inline HashTableEntry *GenericHashTable::FindChainHead(hash_t hash) const {
  const uint64_t pos = hash & mask_;
  return entries_[pos].load(std::memory_order_relaxed);
}

inline HashTableEntry *GenericHashTable::FindChainHeadWithTag(hash_t hash) const {
  const HashTableEntry *const candidate = FindChainHead(hash);
  auto exists_in_chain = reinterpret_cast<uintptr_t>(candidate) & TagHash(hash);
  return (exists_in_chain ? UntagPointer(candidate) : nullptr);
}

template <bool Concurrent>
inline void GenericHashTable::Insert(HashTableEntry *new_entry, hash_t hash) {
  const auto pos = hash & mask_;

  TPL_ASSERT(pos < GetCapacity(), "Computed table position exceeds capacity!");
  TPL_ASSERT(new_entry->hash == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load();
    do {
      new_entry->next = old_entry;
    } while (!loc.compare_exchange_weak(old_entry, new_entry));
  } else {
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load(std::memory_order_relaxed);
    new_entry->next = old_entry;
    loc.store(new_entry, std::memory_order_relaxed);
  }

  num_elems_.fetch_add(1, std::memory_order_relaxed);
}

template <bool Concurrent>
inline void GenericHashTable::InsertTagged(HashTableEntry *new_entry, hash_t hash) {
  const auto pos = hash & mask_;

  TPL_ASSERT(pos < GetCapacity(), "Computed table position exceeds capacity!");
  TPL_ASSERT(new_entry->hash == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load();
    do {
      new_entry->next = UntagPointer(old_entry);
      new_entry = UpdateTag(old_entry, new_entry);
    } while (!loc.compare_exchange_weak(old_entry, new_entry));

  } else {
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load(std::memory_order_relaxed);
    new_entry->next = UntagPointer(old_entry);
    loc.store(UpdateTag(old_entry, new_entry), std::memory_order_relaxed);
  }

  num_elems_.fetch_add(1, std::memory_order_relaxed);
}

template <typename F>
inline void GenericHashTable::FlushEntries(const F &sink) {
  static_assert(std::is_invocable_v<F, HashTableEntry *>);

  for (uint64_t idx = 0; idx < capacity_; idx++) {
    HashTableEntry *entry = entries_[idx].load(std::memory_order_relaxed);
    while (entry != nullptr) {
      HashTableEntry *next = entry->next;
      sink(entry);
      entry = next;
    }
    entries_[idx].store(nullptr, std::memory_order_relaxed);
  }

  num_elems_ = 0;
}

// ---------------------------------------------------------
// Generic Hash Table Iterator
// ---------------------------------------------------------

/**
 * An iterator over the entries in a generic hash table. It's assumed that the underlying hash table
 * is not modified during iteration. This is mostly true for SQL processing where the hash tables
 * are WORM structures.
 * @tparam UseTag Should the iterator use tagged reads?
 */
template <bool UseTag>
class GenericHashTableIterator {
 public:
  /**
   * Construct an iterator over the given hash table @em table.
   * @param table The table to iterate over.
   */
  explicit GenericHashTableIterator(const GenericHashTable &table) noexcept
      : table_(table), entries_index_(0), curr_entry_(nullptr) {
    Next();
  }

  /**
   * @return True if there is more data in the iterator; false otherwise.
   */
  bool HasNext() const noexcept { return curr_entry_ != nullptr; }

  /**
   * Advance the iterator one element.
   */
  void Next() noexcept {
    // If the current entry has a next link, use that
    if (curr_entry_ != nullptr) {
      curr_entry_ = curr_entry_->next;
      if (curr_entry_ != nullptr) {
        return;
      }
    }

    // While we haven't exhausted the directory, and haven't found a valid entry
    // continue on ...
    while (entries_index_ < table_.GetCapacity()) {
      curr_entry_ = table_.entries_[entries_index_++].load(std::memory_order_relaxed);

      if constexpr (UseTag) {
        curr_entry_ = GenericHashTable::UntagPointer(curr_entry_);
      }

      if (curr_entry_ != nullptr) {
        return;
      }
    }
  }

  /**
   * @return The element the iterator is currently pointing to.
   */
  const HashTableEntry *GetCurrentEntry() const noexcept { return curr_entry_; }

 private:
  // The table we're iterating over
  const GenericHashTable &table_;
  // The index into the hash table's entries directory to read from next
  uint64_t entries_index_;
  // The current entry the iterator is pointing to
  const HashTableEntry *curr_entry_;
};

// ---------------------------------------------------------
// Generic Hash Table Vector Iterator
// ---------------------------------------------------------

/**
 * An iterator over a generic hash table that works vector-at-a-time. It's assumed that the
 * underlying hash table is not modified during iteration. This is mostly true for SQL processing
 * where the hash tables are WORM structures.
 * @tparam UseTag Should the iterator use tagged reads?
 */
// TODO(pmenon): Fix my performance
template <bool UseTag>
class GenericHashTableVectorIterator {
 public:
  /**
   * Construct an iterator over the given hash table @em table.
   * @param table The table to iterate over.
   * @param memory The memory pool to use for allocations
   */
  GenericHashTableVectorIterator(const GenericHashTable &table, MemoryPool *memory) noexcept;

  /**
   * Deallocate the entry cache array
   */
  ~GenericHashTableVectorIterator();

  /**
   * @return True if there's more data in the iterator; false otherwise.
   */
  bool HasNext() const { return entry_vec_end_idx_ > 0; }

  /**
   * Advance the iterator by once vector's worth of data.
   */
  void Next();

  /**
   * @return The current batch of entries and its size.
   */
  std::pair<uint16_t, const HashTableEntry **> GetCurrentBatch() const {
    return std::make_pair(entry_vec_end_idx_, entry_vec_);
  }

 private:
  // Pool to use for memory allocations
  MemoryPool *memory_;

  // The hash table we're iterating over
  const GenericHashTable &table_;

  // The index into the hash table's entries directory to read from next
  uint64_t table_dir_index_;

  // The temporary cache of valid entries, and indexes into the entry cache pointing to the current
  // and last valid entry.
  const HashTableEntry **entry_vec_;
  uint16_t entry_vec_end_idx_;
};

}  // namespace tpl::sql
