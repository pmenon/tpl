#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "sql/chaining_hash_table.h"
#include "sql/concise_hash_table.h"
#include "sql/memory_pool.h"
#include "util/chunked_vector.h"
#include "util/spin_latch.h"

namespace libcount {
class HLL;
}  // namespace libcount

namespace tpl::sql {

class JoinHashTableIterator;        // Declared at end.
class JoinHashTableVectorIterator;  // Declared at end.
class ThreadStateContainer;
class Vector;
class VectorProjection;

/**
 * General:
 * --------
 * The main class used to for hash joins. JoinHashTables are bulk-loaded through calls to
 * JoinHashTable::AllocInputTuple() and lazily built through JoinHashTable::Build(). After a
 * JoinHashTable has been built once, it is frozen and immutable. Thus, they're write-once read-many
 * (WORM) structures.
 *
 * @code
 * JoinHashTable jht = ...
 * for (tuple in table) {
 *   auto tuple = reinterpret_cast<YourTuple *>(jht.AllocInputTuple());
 *   tuple->col_a = ...
 *   ...
 * }
 * // All insertions complete, lazily build the table
 * jht.Build();
 * @endcode
 *
 * In parallel mode, thread-local join hash tables are populated and merged in parallel into a
 * global join hash table through a call to JoinHashTable::MergeParallel(). An important difference
 * in the parallel case in contrast to the serial case is that thread-local tables do not call
 * Build()! After thread-local tables have been merged into one global hash table, the global table
 * takes ownership of all thread-local allocated memory and hash index.
 *
 * Lookup:
 * -------
 *
 *
 * Iteration:
 * ----------
 * Tuple-at-a-time iteration is facilitated using JoinHashTableIterator. Iteration only works if the
 * JoinHashTable has been finalized either through Build() or MergeParallel().
 */
class JoinHashTable {
  friend class JoinHashTableIterator;

 public:
  /** Default precision to use for HLL estimations. */
  static constexpr uint32_t kDefaultHLLPrecision = 10;

  /** Minimum number of expected elements to merge before triggering a parallel merge. */
  static constexpr uint32_t kDefaultMinSizeForParallelMerge = 1024;

  /** Statistics structure used to capture information during compression. */
  class AnalysisStats {
   public:
    /** Default constructor. */
    AnalysisStats() = default;

    /** Constructor with specified column count. */
    explicit AnalysisStats(std::size_t num_cols) : bits_(num_cols, 0) {}

    /** Explicitly set column counts. */
    void SetNumCols(std::size_t num_cols) { bits_.resize(num_cols); }

    /** @return The number of columns. */
    std::size_t NumCols() const noexcept { return bits_.size(); }

    /** @return The (current) number of bits required for the column at the given index. */
    uint16_t BitsForCol(std::size_t idx) const noexcept { return bits_[idx]; }

    /** Set the bits for a given column. */
    void SetBitsForCol(std::size_t idx, uint16_t b) noexcept { bits_[idx] = b; }

    /** Merge the given stats structure with this. */
    void Merge(const AnalysisStats &other) {
      SetNumCols(other.NumCols());  // Blindly set column count.
      for (uint32_t i = 0; i < bits_.size(); i++) {
        bits_[i] = std::max(bits_[i], other.bits_[i]);
      }
    }

    /** @return The total number of bits for all columns. */
    uint32_t TotalNumBits() const { return std::accumulate(bits_.begin(), bits_.end(), 0u); }

    /** @return This statistics structure after merging the provided stats. */
    AnalysisStats &operator+=(const AnalysisStats &other) {
      Merge(other);
      return *this;
    }

    /** @return A new statistics structure that merges this and the provided stats. */
    AnalysisStats operator+(const AnalysisStats &other) const {
      AnalysisStats result = *this;
      result += other;
      return result;
    }

   private:
    std::vector<uint16_t> bits_;  // Number of bits for each column.
  };

  /** The structure used to materialized build tuples. */
  using TupleBuffer = util::ChunkedVector<MemoryPoolAllocator<byte>>;

  /** The structure used to collect/store multiple tuple buffers. */
  using TupleBufferVector = MemPoolVector<TupleBuffer>;

  /** Update an analysis struct. */
  using AnalysisPass = void (*)(uint32_t, const byte **, AnalysisStats *);

  /** Compress a set of input data into an output buffer. */
  using CompressPass = void (*)(uint32_t, const byte **, byte **);

  /**
   * Construct a join hash table. All memory allocations are sourced from the injected @em memory,
   * and thus, are ephemeral.
   * @param memory The memory pool to allocate memory from.
   * @param tuple_size The size of the tuple stored in this join hash table.
   * @param use_concise_ht Whether to use a concise or fatter chaining join index.
   */
  explicit JoinHashTable(MemoryPool *memory, uint32_t tuple_size, bool use_concise_ht = false);

  /**
   * Construct a join hash table. All memory allocations are sourced from the injected @em memory,
   * and thus, are ephemeral.
   * @param memory The memory pool to allocate memory from.
   * @param tuple_size The size of the tuple stored in this join hash table.
   * @param use_concise_ht Whether to use a concise or fatter chaining join index.
   * @param analysis_pass An optional analysis pass to analyze the hash table contents after all
   *                      data has been materialized.
   * @param compress_pass An optional compression pass to perform compression given a packing plan.
   */
  explicit JoinHashTable(MemoryPool *memory, uint32_t tuple_size, bool use_concise_ht,
                         AnalysisPass analysis_pass, CompressPass compress_pass);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(JoinHashTable);

  /**
   * Destructor.
   */
  ~JoinHashTable();

  /**
   * Allocate storage in the hash table for an input tuple whose hash value is @em hash. This
   * function only performs an allocation from the table's memory pool. No insertion into the table
   * is performed, meaning a subsequent JoinHashTable::Lookup() for the entry will not return the
   * inserted entry.
   * @param hash The hash value of the tuple to insert.
   * @return A memory region where the caller can materialize the tuple.
   */
  byte *AllocInputTuple(hash_t hash);

  /**
   * Build and finalize the join hash table. After finalization, no new insertions are allowed and
   * the table becomes read-only. Nothing is done if the join hash table has already been finalized.
   */
  void Build();

  /**
   * Lookup a single entry with hash value @em hash returning an iterator.
   * @tparam UseCHT Should the lookup use the concise or general table.
   * @param hash The hash value of the element to lookup.
   * @return An iterator over all elements that match the hash.
   */
  template <bool UseCHT>
  const HashTableEntry *Lookup(hash_t hash) const;

  /**
   * Perform a bulk lookup of tuples whose hash values are stored in @em hashes, storing the results
   * in @em results. The results vector will chain the potentially null head of a chain of
   * HashTableEntry objects.
   * @param hashes The hash values of the probe elements.
   * @param results The heads of the bucket chain of the probed elements.
   */
  void LookupBatch(const Vector &hashes, Vector *results) const;

  /**
   * Merge all thread-local hash tables stored in the state contained into this table. Perform the
   * merge in parallel.
   * @param thread_state_container The container for all thread-local tables.
   * @param jht_offset The offset in the state where the hash table is.
   */
  void MergeParallel(const ThreadStateContainer *thread_state_container, std::size_t jht_offset);

  /**
   * @return The total number of bytes used to materialize tuples. This excludes space required for
   *         the join index.
   */
  uint64_t GetBufferedTupleMemoryUsage() const { return entries_.size() * entries_.element_size(); }

  /**
   * @return The total number of bytes used by the join index only. The join index (also referred to
   *         as the hash table directory), excludes storage for materialized tuple contents.
   */
  uint64_t GetJoinIndexMemoryUsage() const {
    return UsingConciseHashTable() ? concise_hash_table_.GetTotalMemoryUsage()
                                   : chaining_hash_table_.GetTotalMemoryUsage();
  }

  /**
   * @return The total number of bytes used by this table. This includes both the raw tuple storage
   *         and the hash table directory storage.
   */
  uint64_t GetTotalMemoryUsage() const {
    return GetBufferedTupleMemoryUsage() + GetJoinIndexMemoryUsage();
  }

  /**
   * @return The total number of elements in the table, including duplicates.
   */
  uint64_t GetTupleCount() const;

  /**
   * @return True if the join hash table has been built; false otherwise.
   */
  bool IsBuilt() const { return built_; }

  /**
   * @return True if a compression phase should be run; false otherwise.
   */
  bool CompressionSupported() const { return analysis_pass_ != nullptr; }

  /**
   * @return True if this join hash table uses a concise table under the hood.
   */
  bool UsingConciseHashTable() const { return use_concise_ht_; }

 private:
  FRIEND_TEST(JoinHashTableTest, LazyInsertionTest);

  // Access a stored entry by index
  HashTableEntry *EntryAt(const uint64_t idx) {
    return reinterpret_cast<HashTableEntry *>(entries_[idx]);
  }

  const HashTableEntry *EntryAt(const uint64_t idx) const {
    return reinterpret_cast<const HashTableEntry *>(entries_[idx]);
  }

  // Dispatched from Build() to attempt to compress the data before building
  // the physical hash index.
  void TryCompress();
  bool ShouldCompress(const AnalysisStats &stats) const;
  AnalysisStats AnalyzeBufferedTuples() const;
  void CompressBufferedTuples(const AnalysisStats &stats);

  // Dispatched from Build() to build either a chaining or concise hash table.
  void BuildChainingHashTable();
  void BuildConciseHashTable();

  // Dispatched from BuildConciseHashTable() to construct the concise hash table
  // and to reorder buffered build tuples in place according to the CHT.
  template <bool PrefetchCHT, bool PrefetchEntries>
  void BuildConciseHashTableInternal();
  template <bool PrefetchCHT, bool PrefetchEntries>
  void ReorderMainEntries();
  template <bool Prefetch, bool PrefetchEntries>
  void ReorderOverflowEntries();
  void VerifyMainEntryOrder();
  void VerifyOverflowEntryOrder();

  // Dispatched from LookupBatch() to lookup from either a chaining or concise
  // hash table in batched manner.
  void LookupBatchInChainingHashTable(const Vector &hashes, Vector *results) const;
  void LookupBatchInConciseHashTable(const Vector &hashes, Vector *results) const;

  // Merge the source hash table (which isn't built yet) into this one.
  template <bool Concurrent>
  void MergeIncomplete(JoinHashTable *source);

  // Try to compress thread-local hash tables.
  // Called during parallel build.
  void TryCompressParallel(const std::vector<JoinHashTable *> &tables) const;

 private:
  // The optional analysis pass.
  AnalysisPass analysis_pass_;
  // The optional compression pass.
  CompressPass compress_pass_;
  // The container storing the build-side input tuples.
  TupleBuffer entries_;
  // To protect concurrent access to 'owned_entries_'.
  mutable util::SpinLatch owned_latch_;
  // List of entries this hash table has taken ownership of from other tables.
  // Protected by 'owned_latch_'.
  TupleBufferVector owned_;
  // The chaining hash table.
  TaggedChainingHashTable chaining_hash_table_;
  // The concise hash table.
  ConciseHashTable concise_hash_table_;
  // Estimator of unique elements.
  std::unique_ptr<libcount::HLL> hll_estimator_;
  // Has the hash table been built?
  bool built_;
  // Should we use a concise hash table?
  bool use_concise_ht_;
};

// ---------------------------------------------------------
// JoinHashTable implementation
// ---------------------------------------------------------

template <>
inline const HashTableEntry *JoinHashTable::Lookup<false>(const hash_t hash) const {
  HashTableEntry *entry = chaining_hash_table_.FindChainHead(hash);
  while (entry != nullptr && entry->hash != hash) {
    entry = entry->next;
  }
  return entry;
}

template <>
inline const HashTableEntry *JoinHashTable::Lookup<true>(const hash_t hash) const {
  const auto [found, idx] = concise_hash_table_.Lookup(hash);
  auto entry = (found ? EntryAt(idx) : nullptr);
  // TODO(pmenon): Traverse chain until correct hash is found?
  return entry;
}

inline uint64_t JoinHashTable::GetTupleCount() const {
  // We don't know if this hash table was built in parallel. To be safe, we
  // acquire the lock before checking the owned entries vector. This isn't a
  // performance critical function, so locking should be okay ...
  util::SpinLatch::ScopedSpinLatch latch(&owned_latch_);
  if (!owned_.empty()) {
    uint64_t count = 0;
    for (const auto &entries : owned_) {
      count += entries.size();
    }
    return count;
  }

  return entries_.size();
}

//===----------------------------------------------------------------------===//
//
// Join Hash Table Iterator
//
//===----------------------------------------------------------------------===//

/**
 * A tuple-at-a-time iterator over the contents of a join hash table. The join hash table must be
 * fully built either through a serial call to JoinHashTable::Build() or merged (in parallel) from
 * other join hash tables through JoinHashTable::MergeParallel().
 *
 * Users use the OOP-ish iteration API:
 * for (JoinHashTableIterator iter(table); iter.HasNext(); iter.Next()) {
 *   auto row = iter.GetCurrentRowAs<MyFancyAssType>();
 *   ...
 * }
 */
class JoinHashTableIterator {
 public:
  /**
   * Construct an iterator over the given hash table.
   * @param table The join hash table to iterate.
   */
  explicit JoinHashTableIterator(const JoinHashTable &table);

  /**
   * @return True if there is more data in the iterator; false otherwise.
   */
  bool HasNext() const noexcept { return entry_iter_ != entry_end_; }

  /**
   * Advance to the next tuple.
   */
  void Next() noexcept {
    // Advance the entry iterator by one.
    ++entry_iter_;
    // If we've exhausted the current entry list, find another.
    if (entry_iter_ == entry_end_) {
      FindNextNonEmptyList();
    }
  }

  /**
   * Access the row the iterator is currently positioned at.
   * @pre A previous call to HasNext() must have returned true.
   * @return A read-only opaque byte pointer to the row at the current iteration position.
   */
  const byte *GetCurrentRow() const noexcept {
    TPL_ASSERT(HasNext(), "HasNext() indicates no more data!");
    const auto entry = reinterpret_cast<const HashTableEntry *>(*entry_iter_);
    return entry->payload;
  }

  /**
   * Access the row the iterator is currently positioned at as the given template type.
   * @pre A previous call to HasNext() must have returned true.
   * @tparam T The type of the row.
   * @return A typed read-only pointer to row at the current iteration position.
   */
  template <typename T>
  const T *GetCurrentRowAs() const noexcept {
    return reinterpret_cast<const T *>(GetCurrentRow());
  }

 private:
  // Advance past any empty entry lists.
  void FindNextNonEmptyList();

 private:
  using EntryListIterator = JoinHashTable::TupleBufferVector::const_iterator;
  using EntryIterator = JoinHashTable::TupleBuffer::const_iterator;

  // An iterator over the entry lists owned by the join hash table.
  EntryListIterator entry_list_iter_, entry_list_end_;
  // An iterator over the entries in a single entry list.
  EntryIterator entry_iter_, entry_end_;
};

/**
 * A vector-at-a-time iterator over the contents of a join hash table. The table must be fully
 * built either through JoinHashTable::Build(), or have been merged in parallel from other hash
 * tables through JoinHashTable::MergeParallel().
 *
 * Users use the OOP-ish iteration API:
 * for (JoinHashTableVectorIterator iter(table); iter.HasNext(); iter.Next()) {
 *   auto payloads = iter.GetEntries(); // A vector of rows.
 *   ...
 * }
 *
 * The vectors produced in each iteration contain pointers to payload rows.
 *
 * TODO(pmenon): Performance.
 */
class JoinHashTableVectorIterator {
 public:
  /**
   * Create a new vector iterator.
   * @param table The table to iterate over.
   */
  explicit JoinHashTableVectorIterator(const JoinHashTable &table);

  /**
   * Destructor.
   */
  ~JoinHashTableVectorIterator();

  /**
   * @return True if there is more data; false otherwise.
   */
  bool HasNext() const noexcept;

  /**
   * Advance to the next batch of data.
   */
  void Next() noexcept;

  /**
   * @return The current vector of entries.
   */
  Vector *GetEntries() const;

 private:
  // Refill the projection with new data.
  void FillProjection();

 private:
  // The tuple-at-a-time iterator.
  JoinHashTableIterator iter_;
  // The generated projection of entries.
  std::unique_ptr<VectorProjection> data_;
};

}  // namespace tpl::sql
