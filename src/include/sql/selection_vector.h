#pragma once

#include <algorithm>
#include <iosfwd>
#include <string>

#include "common/common.h"
#include "common/macros.h"

namespace tpl::sql {

class SelectionVector {
 public:
  using Iterator = sel_t *;
  using ConstIterator = const sel_t *;

  /**
   * Create an empty selection vector.
   */
  SelectionVector() : sel_vector_{0}, size_(0) {}

  // REMOVE ME
  sel_t *GetRawSelections() { return sel_vector_;}

  /**
   * @return The number of elements in the selection vector.
   */
  uint32_t GetSize() const noexcept { return size_; }

  /**
   * @return True if the vector is empty; false otherwise.
   */
  bool IsEmpty() const noexcept { return GetSize() == 0; }

  /**
   * Clear the selection vector.
   */
  void Clear() noexcept { size_ = 0; }

  /**
   * Unconditionally append the index to the end of the selection vector.
   * @param index The index to append to the end of the list.
   */
  void Append(sel_t index) noexcept {
    TPL_ASSERT(size_ < kDefaultVectorSize, "Too many elements in vector");
    sel_vector_[size_++] = index;
    CheckIntegrity();
  }

  /**
   * Conditionally append the index to the end of the list.
   * @param index The index to append to the end of the list.
   * @param cond The condition used to control if the index is added.
   */
  void AppendConditional(const sel_t index, const bool cond) noexcept {
    TPL_ASSERT(size_ < kDefaultVectorSize, "Too many elements in vector");
    if (cond) sel_vector_[size_++] = index;
    CheckIntegrity();
  }

  /**
   * Iterate over all indexes in the selection vector.
   * @tparam F Functor
   * @param f The callback functor
   */
  template <typename F>
  void ForEach(F f) const {
    for (uint32_t i = 0; i < size_; i++) {
      f(i);
    }
  }

  /**
   * Filter the TIDs in this selection vector based on the given unary filtering function.
   * @tparam P A unary functor that accepts a 32-bit tuple ID and returns true if the tuple ID
   *           remains in the list, and false if the tuple should be removed from the list.
   * @param p The filtering function.
   */
  template <typename P>
  void Filter(P p) {
    uint32_t k = 0;
    for (uint32_t i = 0; i < size_; i++) {
      const sel_t tid = sel_vector_[i];
      if (p(tid)) {
        sel_vector_[k++] = tid;
      }
    }
    size_ = k;
    CheckIntegrity();
  }

  /**
   * Filter the TIDs in this list based on the given unary predicate. If the predicate returns true,
   * the TID remains in this vector. If the predicate returns false, the TID is appended to the
   * @em failures selection vector.
   * @tparam P A unary functor that accepts a 32-bit tuple ID and returns true if the tuple ID
   *           remains in the list, and false if the tuple should be removed from the list.
   * @param p The filtering function.
   */
  template <typename P>
  void SplitFilter(SelectionVector *failures, P p) {
    uint32_t k = 0;
    for (uint32_t i = 0; i < size_; i++) {
      const sel_t tid = sel_vector_[i];
      if (p(tid)) {
        sel_vector_[k++] = tid;
      } else {
        failures->Append(tid);
      }
    }
    size_ = k;
    CheckIntegrity();
  }

  /**
   * Build a selection vector using the indexes of set bits from the input bit vector.
   * @param bits The bits of the bit vector.
   * @param num_bits The number of bits in the bit vector.
   */
  void BuildFromBitVector(const uint64_t *bits, uint32_t num_bits);

  /**
   * @return A string representation of this selection vector.
   */
  std::string ToString() const;

  /**
   * Write a string representation of this selection vector to the provided output stream.
   * @param os The stream to write this vector into.
   */
  void Dump(std::ostream &os) const;

  /**
   * Perform an integrity check of the internal state of this selection vector. This is used in
   * debug mode for sanity checks.
   */
  void CheckIntegrity() const noexcept {
#ifndef NDEBUG
    TPL_ASSERT(std::all_of(sel_vector_, sel_vector_ + size_,
                           [](const auto i) { return i < kDefaultVectorSize; }),
               "Some TIDs are not in range [0, kDefaultVectorSize)");
#endif
  }

  // -------------------------------------------------------
  //
  // Operator Overloads
  //
  // -------------------------------------------------------

  /**
   * Access an element in the list by it's index in the total order.
   * @param i The index of the element to select.
   * @return The value of the element at the given index.
   */
  std::size_t operator[](const uint32_t i) const {
    TPL_ASSERT(i < GetSize(), "Out-of-bounds list access");
    return sel_vector_[i];
  }

  /**
   * Assign the TIDs in @em tids to this selection vector. Any previous contents are cleared out.
   *
   * NOTE: THIS IS ONLY FOR TESTING. DO NOT USE OTHERWISE!
   *
   * @param tids TIDs to add to the list.
   * @return This list.
   */
  SelectionVector &operator=(std::initializer_list<uint32_t> tids) {
    if (!std::all_of(tids.begin(), tids.end(), [](auto tid) { return tid < kDefaultVectorSize; })) {
      throw std::out_of_range("Not all TIDs can be added to this list");
    }

    Clear();
    for (const auto tid : tids) {
      Append(tid);
    }

    return *this;
  }

  // -------------------------------------------------------
  //
  // STL Iterators
  //
  // -------------------------------------------------------

  /**
   * @return An iterator positioned at the first element in this selection vector.
   */
  ConstIterator begin() { return sel_vector_; }

  /**
   * @return A const iterator position at the first element in this selection vector.
   */
  ConstIterator begin() const { return sel_vector_; }

  /**
   * @return An iterator positioned at the end of this selection vector.
   */
  ConstIterator end() { return sel_vector_ + size_; }

  /**
   * @return A const iterator position at the end of this selection vector.
   */
  ConstIterator end() const { return sel_vector_ + size_; }

 private:
  // The array of indexes.
  sel_t sel_vector_[kDefaultVectorSize];
  // The number of elements in the vector.
  sel_t size_;
};

}  // namespace tpl::sql
