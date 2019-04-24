#pragma once

#include "util/chunked_vector.h"

namespace tpl::sql {

class SorterIterator;

/// A sorter
class Sorter {
 public:
  friend class SorterIterator;

  using ComparisonFunction = i32 (*)(const byte *lhs, const byte *rhs);

  /// Construct a sorter using the given allocator, configured to store input
  /// tuples of size \a tuple_size bytes
  Sorter(util::Region *region, ComparisonFunction cmp_fn, u32 tuple_size);

  /// Destructor
  ~Sorter();

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(Sorter);

  /// Allocate space for an entry in this sorter, returning a pointer with
  /// at least \a tuple_size contiguous bytes
  byte *AllocInputTuple();

  /// Tuple allocation for TopK
  byte *AllocInputTupleTopK(u64 top_k);
  void AllocInputTupleTopKFinish(u64 top_k);

  /// Sort all inserted entries
  void Sort();

 private:
  /// Build a max heap from the tuples currently stored in the sorter instance
  void BuildHeap();

  /// Sift down the element at the root of the heap while maintaining the heap
  /// property
  void HeapSiftDown();

 private:
  // Vector of entries
  util::ChunkedVector tuple_storage_;

  // The comparison function
  ComparisonFunction cmp_fn_;

  // Vector of pointers to each entry. This is the vector that's sorted.
  util::ChunkedVectorT<const byte *> tuples_;

  // Flag indicating if the contents of the sorter have been sorted
  bool sorted_;
};

/// An iterator over the elements in a sorter instance
class SorterIterator {
 public:
  explicit SorterIterator(Sorter *sorter) noexcept
      : iter_(sorter->tuples_.begin()), end_(sorter->tuples_.end()) {}

  // -------------------------------------------------------
  // C++ iterator interface
  // -------------------------------------------------------

  const byte *operator*() const noexcept { return *iter_; }

  SorterIterator &operator++() noexcept {
    ++iter_;
    return *this;
  }

  /// Does this iterate have more data
  /// \return True if the iterator has more data; false otherwise
  bool HasNext() const { return iter_ != end_; }

  /// Advance the iterator
  void Next() { this->operator++(); }

  /// Get a pointer to the row the iterator is pointing to
  /// Note: This is unsafe at boundaries
  const byte *GetRow() const { return this->operator*(); }

 private:
  util::ChunkedVectorT<const byte *>::Iterator iter_;
  const util::ChunkedVectorT<const byte *>::Iterator end_;
};

}  // namespace tpl::sql
