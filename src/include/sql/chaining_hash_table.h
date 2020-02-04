#pragma once

#include <atomic>
#include <tuple>
#include <utility>

#include "common/common.h"
#include "common/cpu_info.h"
#include "common/macros.h"
#include "common/memory.h"
#include "sql/hash_table_entry.h"
#include "sql/memory_pool.h"
#include "util/chunked_vector.h"

namespace tpl::sql {

//===----------------------------------------------------------------------===//
//
// Generic Hash Table
//
//===----------------------------------------------------------------------===//

/**
 * ChainingHashTable is a simple bucket-chained table with optional pointer tagging. The use of
 * pointer tagging is controlled through the sole boolean template parameter. Pointer tagging uses
 * the first @em GenericHashTable::kNumTagBits bits (typically 16 bits) of the entry pointers in the
 * main bucket directory as a tiny bloom filter. This bloom filter is used to early-prune probe
 * misses that would normally require a full cache-unfriendly linked list traversal. We can
 * re-purpose the most-significant 16-bits of a pointer because X86_64 uses 48-bits of the VM
 * address space. Pointer tagged hash tables will have to be disabled when the full 64-bit VM
 * address space is enabled.
 *
 * GenericHashTable support both serial and concurrent insertions. Both are controlled through
 * a template parameter to GenericHashTable::Insert() and GenericHashTable::InsertBatch(). The
 * former method inserts a single entry, while the latter method bulk-loads a batch of entries.
 * Whenever possible, prefer using the latter as it can apply some optimizations that usually
 * improve performance, such as prefetching and batched atomic additions.
 *
 * GenericHashTable also supports both one-at-a-time and batched probes. Like batched insertion,
 * prefer using the batched probe because it offers greater performance.
 *
 * GenericHashTables only stores pointers into externally managed storage; it does not manage any
 * hash table data internally. In other words, the memory of all inserted HashTableEntry must be
 * owned by an external entity whose lifetime exceeds this GenericHashTable!
 *
 * Note: GenericHashTable leverages the ‘next’ pointer in HashTableEntry::next to implement the
 * linked list bucket chain.
 */
template <bool UseTags>
class ChainingHashTable {
 private:
  // X86_64 has 48-bit VM address space, leaving 16 for us to re-purpose.
  static constexpr uint32_t kNumTagBits = 16;
  // The number of bits to use for the physical pointer.
  static constexpr uint32_t kNumPointerBits = (sizeof(void *) * kBitsPerByte) - kNumTagBits;
  // The mask to use to retrieve the physical pointer from a tagged pointer.
  static constexpr uint64_t kMaskPointer = (~0ull) >> kNumTagBits;
  // The mask to use to retrieve the tag from a tagged pointer.
  static constexpr uint64_t kMaskTag = (~0ull) << kNumPointerBits;

  // The default load factor to use.
  static constexpr float kDefaultLoadFactor = 0.7;

 public:
  /**
   * Create an empty hash table. Callers must first call GenericHashTable<UseTags>::SetSize() before
   * using this hash table.
   * @param load_factor The desired load-factor for the table.
   */
  explicit ChainingHashTable(float load_factor = kDefaultLoadFactor) noexcept;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ChainingHashTable);

  /**
   * Destructor.
   */
  ~ChainingHashTable();

  /**
   * Explicitly set the size of the hash table to support at least @em new_size elements. The input
   * size @em new_size serves as a lower-bound of the expected number of elements. This resize
   * operation may resize to a larger value to (1) respect the load factor or to (2) ensure a power
   * of two size.
   * @param new_size The expected number of elements that will be inserted into the table.
   */
  void SetSize(uint64_t new_size);

  /**
   * Insert an entry into the hash table without tagging the entry.
   *
   * @pre The input hash value @em hash should match what's stored in @em entry.
   *
   * @tparam Concurrent Is the insert occurring concurrently with other inserts.
   * @param entry The entry to insert.
   * @param hash The hash value of the entry.
   */
  template <bool Concurrent>
  void Insert(HashTableEntry *entry);

  /**
   * Insert a list of entries into this hash table.
   * @tparam Concurrent Is the insertion occurring concurrently with other insertions?
   * @tparam Allocator The allocator used by the vector.
   * @param entries The list of entries to insert into the table.
   */
  template <bool Concurrent, typename Allocator>
  void InsertBatch(util::ChunkedVector<Allocator> *entries);

  /**
   * Prefetch the head of the bucket chain for the hash @em hash.
   * @tparam ForRead Whether the prefetch is intended for a subsequent read.
   * @param hash The hash value of the element to prefetch.
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
   * Perform a batch lookup of elements whose hash values are stored in @em hashes, storing the
   * results in @em results.
   * @param num_elements The number of hashes to lookup.
   * @param hashes The hash values of the probe elements.
   * @param results The heads of the bucket chain of the probed elements.
   */
  void LookupBatch(uint64_t num_elements, const hash_t hashes[],
                   const HashTableEntry *entries[]) const;

  /**
   * Empty all entries in this hash table into the sink functor. After this function exits, the hash
   * table is empty.
   * @tparam F The function must be of the form void(*)(HashTableEntry*)
   * @param sink The sink of all entries in the hash table
   */
  template <typename F>
  void FlushEntries(const F &sink);

  /**
   * @return True if the hash table is empty; false otherwise.
   */
  bool IsEmpty() const { return GetElementCount() == 0; }

  /**
   * @return The total number of bytes this hash table has allocated.
   */
  uint64_t GetTotalMemoryUsage() const { return sizeof(HashTableEntry *) * GetCapacity(); }

  /**
   * @return The number of elements stored in this hash table.
   */
  uint64_t GetElementCount() const { return num_elements_.load(std::memory_order_relaxed); }

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

  /**
   * @return Collect and return a tuple containing the minimum, maximum, and average bucket chain in
   *         this hash table. This is not a concurrent operation!
   */
  std::tuple<uint64_t, uint64_t, float> GetChainLengthStats() const;

 private:
  template <bool>
  friend class ChainingHashTableIterator;
  template <bool>
  friend class ChainingHashTableVectorIterator;

  // Return the position of the bucket the given hash value lands into
  uint64_t BucketPosition(const hash_t hash) const { return hash & mask_; }

  // Add the given value to the total element count
  void AddElementCount(uint64_t v) { num_elements_.fetch_add(v, std::memory_order_relaxed); }

  // Note: internal insertion functions do not modify the element count!

  // Insert an entry into the hash table without tagging the entry.
  template <bool Concurrent>
  void InsertUntagged(HashTableEntry *entry, hash_t hash);

  // Insert an entry into the hash table using a tagged pointers.
  template <bool Concurrent>
  void InsertTagged(HashTableEntry *entry, hash_t hash);

  // Insert a list of entries into the hash table.
  template <bool Prefetch, bool Concurrent, typename Allocator>
  void InsertBatchInternal(util::ChunkedVector<Allocator> *entries);

  // Given a hash value, return the head of the bucket chain ignoring any tag.
  // This probe is performed assuming no concurrent access into the table.
  HashTableEntry *FindChainHeadUntagged(hash_t hash) const;

  // Given a hash value, return the head of the bucket chain removing the tag.
  // This probe is performed assuming no concurrent access into the table.
  HashTableEntry *FindChainHeadTagged(hash_t hash) const;

  // Perform a batch lookup of elements.
  template <bool Prefetch>
  void LookupBatchInternal(uint64_t num_elements, const hash_t hashes[],
                           const HashTableEntry *entries[]) const;

  // -------------------------------------------------------
  // Tag-related operations
  // -------------------------------------------------------

  // Given a tagged HashTableEntry pointer, strip out the tag bits and return an
  // untagged HashTableEntry pointer
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
    // We use the given hash value to obtain a bit position in the tag to set.
    // We need to extract a signature from the hash value in the range
    // [0, kNumTagBits), so we take the log2(kNumTagBits) most significant bits
    // to determine which bit in the tag to set.
    auto tag_bit_pos = hash >> (sizeof(hash_t) * 8 - 4);
    TPL_ASSERT(tag_bit_pos < kNumTagBits, "Invalid tag!");
    return 1ull << (tag_bit_pos + kNumPointerBits);
  }

 private:
  // Main directory of hash table entry buckets. Each bucket is the head of a
  // linked list chain.
  HashTableEntry **entries_;

  // The mask to use to compute the bucket position of an entry.
  uint64_t mask_;

  // The capacity of the directory.
  uint64_t capacity_;

  // The current number of elements stored in the table.
  std::atomic<uint64_t> num_elements_;

  // The configured load-factor.
  float load_factor_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

template <bool UseTags>
template <bool ForRead>
inline void ChainingHashTable<UseTags>::PrefetchChainHead(hash_t hash) const {
  const uint64_t pos = BucketPosition(hash);
  Memory::Prefetch<ForRead, Locality::Low>(entries_ + pos);
}

#define COMPARE_EXCHANGE_WEAK(ADDRESS, EXPECTED, NEW_VAL)                                     \
  (__atomic_compare_exchange_n((ADDRESS),        /* Address to atomically CAS into */         \
                               (EXPECTED),       /* The old value we read from the address */ \
                               (NEW_VAL),        /* The new value we want to write there */   \
                               true,             /* Weak exchange ?*/                         \
                               __ATOMIC_RELEASE, /* Use release semantics for success*/       \
                               __ATOMIC_RELAXED  /* Use relaxed semantics for failure*/       \
                               ))

template <bool UseTags>
template <bool Concurrent>
inline void ChainingHashTable<UseTags>::InsertUntagged(HashTableEntry *const entry,
                                                       const hash_t hash) {
  const uint64_t pos = BucketPosition(hash);

  TPL_ASSERT(pos < GetCapacity(), "Computed table position exceeds capacity!");
  TPL_ASSERT(entry->hash == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    HashTableEntry *old_entry = entries_[pos];
    do {
      entry->next = old_entry;
    } while (!COMPARE_EXCHANGE_WEAK(entries_ + pos, &old_entry, entry));
  } else {
    entry->next = entries_[pos];
    entries_[pos] = entry;
  }
}

template <bool UseTags>
template <bool Concurrent>
inline void ChainingHashTable<UseTags>::InsertTagged(HashTableEntry *const entry,
                                                     const hash_t hash) {
  const uint64_t pos = BucketPosition(hash);

  TPL_ASSERT(pos < GetCapacity(), "Computed table position exceeds capacity!");
  TPL_ASSERT(entry->hash == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    HashTableEntry *old_entry = entries_[pos];
    HashTableEntry *new_entry = nullptr;
    do {
      entry->next = UntagPointer(old_entry);    // Un-tag the old entry
      new_entry = UpdateTag(old_entry, entry);  // Tag the new entry
    } while (!COMPARE_EXCHANGE_WEAK(entries_ + pos, &old_entry, new_entry));
  } else {
    entry->next = UntagPointer(entries_[pos]);
    entries_[pos] = UpdateTag(entries_[pos], entry);
  }
}

#undef COMPARE_EXCHANGE_WEAK

template <bool UseTags>
template <bool Concurrent>
inline void ChainingHashTable<UseTags>::Insert(HashTableEntry *const entry) {
  if constexpr (UseTags) {
    InsertTagged<Concurrent>(entry, entry->hash);
  } else {
    InsertUntagged<Concurrent>(entry, entry->hash);
  }

  // Update element count
  AddElementCount(1);
}

template <bool UseTags>
template <bool Prefetch, bool Concurrent, typename Allocator>
inline void ChainingHashTable<UseTags>::InsertBatchInternal(
    util::ChunkedVector<Allocator> *entries) {
  const uint64_t size = entries->size();
  for (uint64_t idx = 0, prefetch_idx = kPrefetchDistance; idx < size; idx++, prefetch_idx++) {
    if constexpr (Prefetch) {
      if (TPL_LIKELY(prefetch_idx < size)) {
        auto *prefetch_entry = reinterpret_cast<HashTableEntry *>((*entries)[prefetch_idx]);
        PrefetchChainHead<false>(prefetch_entry->hash);
      }
    }

    auto *entry = reinterpret_cast<HashTableEntry *>((*entries)[idx]);
    if constexpr (UseTags) {
      InsertTagged<Concurrent>(entry, entry->hash);
    } else {
      InsertUntagged<Concurrent>(entry, entry->hash);
    }
  }
}

template <bool UseTags>
template <bool Concurrent, typename Allocator>
inline void ChainingHashTable<UseTags>::InsertBatch(util::ChunkedVector<Allocator> *entries) {
  const uint64_t l3_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (bool out_of_cache = GetTotalMemoryUsage() > l3_size; out_of_cache) {
    InsertBatchInternal<true, Concurrent>(entries);
  } else {
    InsertBatchInternal<false, Concurrent>(entries);
  }

  // Update element count
  AddElementCount(entries->size());
}

template <bool UseTags>
inline HashTableEntry *ChainingHashTable<UseTags>::FindChainHeadUntagged(hash_t hash) const {
  const uint64_t pos = BucketPosition(hash);
  return entries_[pos];
}

template <bool UseTags>
inline HashTableEntry *ChainingHashTable<UseTags>::FindChainHeadTagged(hash_t hash) const {
  const HashTableEntry *const candidate = FindChainHeadUntagged(hash);
  auto exists_in_chain = reinterpret_cast<uintptr_t>(candidate) & TagHash(hash);
  return (exists_in_chain ? UntagPointer(candidate) : nullptr);
}

template <bool UseTags>
inline HashTableEntry *ChainingHashTable<UseTags>::FindChainHead(hash_t hash) const {
  if constexpr (UseTags) {
    return FindChainHeadTagged(hash);
  } else {
    return FindChainHeadUntagged(hash);
  }
}

template <bool UseTags>
template <bool Prefetch>
inline void ChainingHashTable<UseTags>::LookupBatchInternal(const uint64_t num_elements,
                                                            const hash_t hashes[],
                                                            const HashTableEntry *entries[]) const {
  for (uint64_t idx = 0, prefetch_idx = kPrefetchDistance; idx < num_elements;
       idx++, prefetch_idx++) {
    if constexpr (Prefetch) {
      if (TPL_LIKELY(prefetch_idx < num_elements)) {
        PrefetchChainHead<true>(hashes[prefetch_idx]);
      }
    }

    if constexpr (UseTags) {
      entries[idx] = FindChainHeadTagged(hashes[idx]);
    } else {
      entries[idx] = FindChainHeadUntagged(hashes[idx]);
    }
  }
}

template <bool UseTags>
inline void ChainingHashTable<UseTags>::LookupBatch(const uint64_t num_elements,
                                                    const hash_t hashes[],
                                                    const HashTableEntry *entries[]) const {
  const uint64_t l3_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (bool out_of_cache = GetTotalMemoryUsage() > l3_size; out_of_cache) {
    LookupBatchInternal<true>(num_elements, hashes, entries);
  } else {
    LookupBatchInternal<false>(num_elements, hashes, entries);
  }
}

template <bool UseTags>
template <typename F>
inline void ChainingHashTable<UseTags>::FlushEntries(const F &sink) {
  static_assert(std::is_invocable_v<F, HashTableEntry *>);

  for (uint64_t idx = 0; idx < capacity_; idx++) {
    HashTableEntry *entry = entries_[idx];

    if constexpr (UseTags) {
      entry = UntagPointer(entry);
    }

    while (entry != nullptr) {
      HashTableEntry *next = entry->next;
      sink(entry);
      entry = next;
    }

    entries_[idx] = nullptr;
  }

  num_elements_ = 0;
}

using TaggedChainingHashTable = ChainingHashTable<true>;
using UntaggedChainingHashTable = ChainingHashTable<false>;

//===----------------------------------------------------------------------===//
//
// Chaining Hash Table Iterator
//
//===----------------------------------------------------------------------===//

/**
 * An iterator over the entries in a generic hash table. It's assumed that the underlying hash table
 * is not modified during iteration. This is mostly true for SQL processing where the hash tables
 * are WORM structures.
 * @tparam UseTag Should the iterator use tagged reads?
 */
template <bool UseTag>
class ChainingHashTableIterator {
 public:
  /**
   * Construct an iterator over the given hash table @em table.
   * @param table The table to iterate over.
   */
  explicit ChainingHashTableIterator(const ChainingHashTable<UseTag> &table) noexcept
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
      curr_entry_ = table_.entries_[entries_index_++];

      if constexpr (UseTag) {
        curr_entry_ = ChainingHashTable<UseTag>::UntagPointer(curr_entry_);
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
  const ChainingHashTable<UseTag> &table_;
  // The index into the hash table's entries directory to read from next
  uint64_t entries_index_;
  // The current entry the iterator is pointing to
  const HashTableEntry *curr_entry_;
};

//===----------------------------------------------------------------------===//
//
// Chaining Hash Table Vector Iterator
//
//===----------------------------------------------------------------------===//

/**
 * An iterator over a generic hash table that works vector-at-a-time. It's assumed that the
 * underlying hash table is not modified during iteration. This is mostly true for SQL processing
 * where the hash tables are WORM structures.
 * @tparam UseTag Should the iterator use tagged reads?
 */
// TODO(pmenon): Fix my performance
template <bool UseTag>
class ChainingHashTableVectorIterator {
 public:
  /**
   * Construct an iterator over the given hash table @em table.
   * @param table The table to iterate over.
   * @param memory The memory pool to use for allocations.
   */
  ChainingHashTableVectorIterator(const ChainingHashTable<UseTag> &table,
                                  MemoryPool *memory) noexcept;

  /**
   * Deallocate the entry cache array
   */
  ~ChainingHashTableVectorIterator();

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
  const ChainingHashTable<UseTag> &table_;

  // The index into the hash table's entries directory to read from next
  uint64_t table_dir_index_;

  // The temporary cache of valid entries, and indexes into the entry cache
  // pointing to the current and last valid entry.
  const HashTableEntry **entry_vec_;
  uint32_t entry_vec_end_idx_;
};

}  // namespace tpl::sql
