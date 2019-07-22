#pragma once

#include <iterator>
#include <memory>
#include <vector>

#include "sql/memory_pool.h"
#include "sql/schema.h"
#include "util/chunked_vector.h"
#include "util/macros.h"

namespace tpl::sql {

class ThreadStateContainer;
class VectorProjection;
class VectorProjectionIterator;

/**
 * Sorters
 */
class Sorter {
 public:
  /**
   * The interface of the comparison function used to sort tuples
   */
  using ComparisonFunction = i32 (*)(const void *lhs, const void *rhs);

  /**
   * Construct a sorter using @em memory as the memory allocator, storing tuples
   * @em tuple_size size in bytes, and using the comparison function @em cmp_fn.
   * @param memory The memory pool to allocate memory from
   * @param cmp_fn The sorting comparison function
   * @param tuple_size The sizes of the input tuples in bytes
   */
  Sorter(MemoryPool *memory, ComparisonFunction cmp_fn, u32 tuple_size);

  /**
   * Destructor
   */
  ~Sorter();

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(Sorter);

  /**
   * Allocate space for an entry in this sorter, returning a pointer with
   * at least \a tuple_size contiguous bytes
   */
  byte *AllocInputTuple();

  /**
   * Tuple allocation for TopK. This call is must be paired with a subsequent
   * @em AllocInputTupleTopKFinish() call.
   *
   * @see AllocInputTupleTopKFinish()
   */
  byte *AllocInputTupleTopK(u64 top_k);

  /**
   * Complete the allocation and insertion of a tuple intended for TopK. This
   * call must be preceded by a call to @em AllocInputTupleTopK().
   *
   * @see AllocInputTupleTopK()
   */
  void AllocInputTupleTopKFinish(u64 top_k);

  /**
   * Sort all inserted entries
   */
  void Sort();

  /**
   * Perform a parallel sort of all sorter instances stored in the thread state
   * container object. Each thread-local sorter instance is assumed (but not
   * required) to be unsorted. Once sorting completes, this sorter instance will
   * take ownership of all data owned by each thread-local instances.
   * @param thread_state_container The container holding all thread-local sorter
   *                               instances.
   * @param sorter_offset The offset into the container where the sorter
   *                      instance is.
   */
  void SortParallel(const ThreadStateContainer *thread_state_container,
                    u32 sorter_offset);

  /**
   * Perform a parallel Top-K of all sorter instances stored in the thread
   * state container object. Each thread-local sorter instance is assumed (but
   * not required) to be unsorted. Once sorting completes, this sorter instance
   * will take ownership of all data owned by each thread-local instances.
   * @param thread_state_container The container holding all thread-local sorter
   *                               instances.
   * @param sorter_offset The offset into the container where the sorter
   *                      instance is.
   * @param top_k The number entries at the top the caller cares for.
   */
  void SortTopKParallel(const ThreadStateContainer *thread_state_container,
                        u32 sorter_offset, u64 top_k);

  /**
   * Return the number of tuples currently in this sorter
   */
  u64 NumTuples() const { return tuples_.size(); }

  /**
   * Has this sorter's contents been sorted?
   */
  bool is_sorted() const { return sorted_; }

 private:
  // Build a max heap from the tuples currently stored in the sorter instance
  void BuildHeap();

  // Sift down the element at the root of the heap while maintaining the heap
  // property
  void HeapSiftDown();

 private:
  friend class SorterIterator;
  friend class SorterVectorIterator;

  // Memory pool
  MemoryPool *memory_;

  // Vector of entries
  util::ChunkedVector<MemoryPoolAllocator<byte>> tuple_storage_;

  // All tuples this sorter has taken ownership of from thread-local sorters
  MemPoolVector<util::ChunkedVector<MemoryPoolAllocator<byte>>> owned_tuples_;

  // The comparison function
  ComparisonFunction cmp_fn_;

  // Vector of pointers to each entry. This is the vector that's sorted.
  MemPoolVector<const byte *> tuples_;

  // Flag indicating if the contents of the sorter have been sorted
  bool sorted_;
};

/**
 * An iterator over the elements in a sorter instance.
 */
class SorterIterator {
  using IteratorType = decltype(Sorter::tuples_)::const_iterator;

 public:
  explicit SorterIterator(const Sorter &sorter) noexcept
      : iter_(sorter.tuples_.begin()), end_(sorter.tuples_.end()) {}

  /**
   * Dereference operator.
   * @return A pointer to the current iteration row.
   */
  const byte *operator*() const noexcept { return GetRow(); }

  /**
   * Pre-increment the iterator.
   * @return A reference to this iterator after it's been advanced one row.
   */
  SorterIterator &operator++() noexcept {
    Next();
    return *this;
  }

  /**
   * Does this iterator have more data.
   * @return True if the iterator has more data; false otherwise.
   */
  bool HasNext() const { return iter_ != end_; }

  /**
   * Advance the iterator.
   */
  void Next() { ++iter_; }

  /**
   * Determine the number of rows remaining in the iteration.
   */
  u32 NumRemaining() const { return std::distance(iter_, end_); }

  /**
   * Return a pointer to the current row. It assumed the called has checked the
   * iterator is valid.
   */
  const byte *GetRow() const {
    TPL_ASSERT(iter_ != end_, "Invalid iterator");
    return *iter_;
  }

  /**
   * Return a pointer to the current row, interpreted as the template type
   * @em T. It assumed the called has checked the iterator is valid.
   */
  template <typename T>
  const T *GetRowAs() const {
    return reinterpret_cast<const T *>(GetRow());
  }

 private:
  // The current iterator position
  IteratorType iter_;
  // The ending iterator position
  const IteratorType end_;
};

/**
 * A vectorized iterator over the elements in a sorter instance.
 */
class SorterVectorIterator {
 public:
  using TransposeFn = void (*)(const byte **, u64, VectorProjectionIterator *);

  /**
   * Construct a vector iterator over the given sorter instance.
   */
  SorterVectorIterator(
      const Sorter &sorter,
      const std::vector<const Schema::ColumnInfo *> &column_info,
      TransposeFn transpose_fn);

  /**
   * Construct a vector iterator over the given sorter instance.
   */
  SorterVectorIterator(const Sorter &sorter,
                       const Schema::ColumnInfo *column_info, u32 num_cols,
                       TransposeFn transpose_fn);

  /**
   * Destructor.
   */
  ~SorterVectorIterator();

  /**
   * Does this iterator have more data?
   */
  bool HasNext() const;

  /**
   * Advance the iterator by, at most, one vector's worth of data.
   */
  void Next(TransposeFn transpose_fn);

  /**
   * Return the next vector output.
   */
  VectorProjectionIterator *GetVectorProjectionIterator() {
    return vector_projection_iterator_.get();
  }

 private:
  void BuildVectorProjection(TransposeFn transpose_fn);

 private:
  // The memory pool
  MemoryPool *memory_;
  // The current and ending iterator positions, respectively.
  SorterIterator iter_;
  // Temporary array storing the sorter rows
  const byte **temp_rows_;
  // The vector projections produced by this iterator
  std::unique_ptr<VectorProjection> vector_projection_;
  // The iterator over the vector projection
  std::unique_ptr<VectorProjectionIterator> vector_projection_iterator_;
};

}  // namespace tpl::sql
