#pragma once

#include <iosfwd>
#include <string>
#include <type_traits>

#include "common/macros.h"
#include "util/bit_vector.h"

namespace tpl::sql {

/**
 * An ordered set of tuple IDs used during query execution to efficiently represent valid tuples in
 * a vector projection. TupleIdLists can represent all TIDs in the range [0, capacity), set up
 * upon construction, but can also be resized afterwards to large or smaller capacities.
 *
 * Checking the existence of a TID in the list is a constant time operation, as is adding or
 * removing (one or all) TIDs from the list. Intersection, union, and difference are efficient
 * operations linear in the capacity of the list.
 *
 * Users can iterate over the TIDs in the list through TupleIdList::Iterate(), and build/update
 * lists from subsets of other lists using TupleIdList::BuildFromOtherList().
 *
 * Tuple ID lists can also be converted into dense selection vectors through
 * TupleIdList::AsSelectionVector().
 *
 * Implementation:
 * ---------------
 * TupleIdList is implemented as a very thin wrapper around a bit-vector. Thus, they occupy 128 or
 * 256 bytes of memory to represent 1024 or 2048 tuples, respectively. Using a bit vector as the
 * underlying data structure enables efficient implementations of list intersection, union, and
 * difference required during expression evaluation. Moreover, bit vectors are amenable to
 * auto-vectorization by the compiler.
 *
 * The primary drawback of bit vectors is iteration: dense RID lists (also known as selection
 * vectors) are faster to iterate over than bit vectors, more so when the selectivity of the vector
 * is low.
 */
class TupleIdList {
 public:
  using BitVectorType = util::BitVector<uint64_t>;

  /**
   * An iterator over the TIDs in a Tuple ID list.
   *
   * @warning While this exists for convenience, it is very slow and should only be used when the
   * loop is driven by an external controller. When possible, design your algorithms around the
   * callback-based iteration functions in TupleIdList such as TupleIdList::Iterate() and
   * TupleIdList::Filter() as they perform more than 3x faster!!!!!
   */
  class ConstIterator {
   public:
    uint32_t operator*() const noexcept { return curr_; }

    ConstIterator &operator++() {
      curr_ = bv_.FindNext(curr_);
      return *this;
    }

    bool operator==(const ConstIterator &that) const noexcept {
      return &bv_ == &that.bv_ && curr_ == that.curr_;
    }

    bool operator!=(const ConstIterator &that) const noexcept { return !(*this == that); }

   private:
    friend class TupleIdList;

    ConstIterator(const BitVectorType &bv, uint32_t position) : bv_(bv), curr_(position) {}

    explicit ConstIterator(const BitVectorType &bv) : ConstIterator(bv, bv.FindFirst()) {}

   private:
    const BitVectorType &bv_;
    uint32_t curr_;
  };

  /**
   * Construct a TID list with the given maximum size.
   * @param size The maximum size of the list.
   * @param is_for_conjunction whether to perform conjections.
   */
  explicit TupleIdList(uint32_t size, bool is_for_conjunction) : bit_vector_(size), is_for_conjunction_{is_for_conjunction} {}

  /**
   * Resize the list to the given size. If growing the list, the contents of the list remain
   * unchanged. If shrinking the list, previously added/active elements are discarded.
   */
  void Resize(uint32_t size) { bit_vector_.Resize(size); }

  /**
   * Is the tuple with the given ID in this list?
   * @param tid The tuple ID to check.
   * @return True if the tuple is in the list; false otherwise.
   */
  bool Contains(const uint32_t tid) const { return bit_vector_.Test(tid); }

  /**
   * Does the list contain all TIDs in the range of TIDs it tracks?
   * @return True if full; false otherwise.
   */
  bool IsFull() const { return bit_vector_.All(); }

  /**
   * Is the list empty?
   * @return True if empty; false otherwise.
   */
  bool IsEmpty() const { return bit_vector_.None(); }

  /**
   * Set a given tuple as active in the list.
   * @param tid The ID of the tuple.
   */
  void Add(const uint32_t tid) { bit_vector_.Set(tid); }

  /**
   * Add all tuples whose IDs are in the range [start_tid, end_tid). Note the half-open interval!
   * @param start_tid The left inclusive range boundary.
   * @param end_tid The right inclusive range boundary.
   */
  void AddRange(const uint32_t start_tid, const uint32_t end_tid) {
    bit_vector_.SetRange(start_tid, end_tid);
  }

  /**
   * Add all tuple IDs this list can support.
   */
  void AddAll() { bit_vector_.SetAll(); }

  /**
   * Enable or disable the tuple with the given ID depending on the value of @em enable. If
   * @em enable is true, the tuple is added to the list, and otherwise it is disabled.
   * @param tid The ID to add or remove from the list.
   * @param enable The flag indicating if the tuple is added or removed.
   */
  void Enable(const uint32_t tid, const bool enable) { bit_vector_.Set(tid, enable); }

  /**
   * Remove the tuple with the given ID from the list.
   * @param tid The ID of the tuple.
   */
  void Remove(const uint32_t tid) { bit_vector_.Unset(tid); }

  /**
   * Intersect the set of tuple IDs in this list with the tuple IDs in the provided list.
   * @param other The list to intersect with.
   */
  void IntersectWith(const TupleIdList &other) { bit_vector_.Intersect(other.bit_vector_); }

  /**
   * Union the set of tuple IDs in this list with the tuple IDs in the provided list.
   * @param other The list to union with.
   */
  void UnionWith(const TupleIdList &other) { bit_vector_.Union(other.bit_vector_); }

  /**
   * Negates the set of tuple IDs.
   */
  void Negate() { bit_vector_.Negate(); }

  /**
   * Remove all tuple IDs from this list that are also present in the provided list.
   * @param other The list to unset from.
   */
  void UnsetFrom(const TupleIdList &other) { bit_vector_.Difference(other.bit_vector_); }

  /**
   * Filter the TIDs in this list based on the given function.
   * @tparam F A functor that accepts a 32-bit tuple ID and returns a boolean.
   * @param f The function that filters the IDs from the input, returning true for valid tuples, and
   *          false otherwise.
   */
  template <typename F>
  void Filter(F &&f) {
    bit_vector_.UpdateSetBits(f);
  }

  /**
   * Build a list of TIDs from the IDs in the input selection vector.
   * @param sel_vector The selection vector.
   * @param size The number of elements in the selection vector.
   */
  void BuildFromSelectionVector(const sel_t *sel_vector, uint32_t size);

  /**
   * Convert the given selection match vector to a TID list. The match vector is assumed to be
   * boolean-like array, but with saturated values. This means that the value 'true' or 1 is
   * physically encoded as all-1s, i.e., a true value is the 8-byte value 255 = 11111111b, and the
   * value 'false' or 0 is encoded as all zeros. This is typically used during selections which
   * naturally produce saturated match vectors.
   * @param matches The match vector.
   * @param size The number of elements in the match vector.
   */
  void BuildFromMatchVector(const uint8_t *const matches, const uint32_t size) {
    bit_vector_.SetFromBytes(matches, size);
  }

  /**
   * Remove all tuples from the list.
   */
  void Clear() { bit_vector_.Reset(); }

  /**
   * @return The number of active tuples in the list.
   */
  uint32_t GetTupleCount() const { return bit_vector_.CountOnes(); }

  /**
   * @return The capacity of the TID list.
   */
  uint32_t GetCapacity() const { return bit_vector_.num_bits(); }

  /**
   * @return The selectivity of the list a fraction in the range [0.0, 1.0].
   */
  float ComputeSelectivity() const { return static_cast<float>(GetTupleCount()) / GetCapacity(); }

  /**
   * Convert this tuple ID list into a dense selection index vector.
   * @param[out] sel_vec The output selection vector.
   * @return The number of elements in the generated selection vector.
   */
  [[nodiscard]] uint32_t AsSelectionVector(sel_t *sel_vec) const;

  /**
   * Iterate all TIDs in this list.
   * @tparam F Functor type which must take a single unsigned integer parameter.
   * @param f The callback to invoke for each TID in the list.
   */
  template <typename F>
  void Iterate(F &&f) const {
    bit_vector_.IterateSetBits(f);
  }

  /**
   * @return A string representation of this list.
   */
  std::string ToString() const;

  /**
   * Print a string representation of this vector to the output stream.
   * @param stream Where the string representation is printed to.
   */
  void Dump(std::ostream &stream) const;

  /**
   * @return The internal bit vector representation of the list.
   */
  BitVectorType *GetMutableBits() { return &bit_vector_; }

  /**
   * Access an element in the list by it's index in the total order.
   * @param i The index of the element to select.
   * @return The value of the element at the given index.
   */
  std::size_t operator[](const std::size_t i) const {
    TPL_ASSERT(i < GetTupleCount(), "Out-of-bounds list access");
    return bit_vector_.NthOne(i);
  }

  /**
   * Does this list contain the same list of TIDs as the provided list @em that?
   * @param that The list to compare with.
   * @return True if the list contain the same contents; false otherwise.
   */
  bool operator==(const TupleIdList &that) const noexcept {
    return bit_vector_ == that.bit_vector_;
  }

  /**
   * Does this list contain any TIDs that differ from the provided list @em that?
   * @param that The list to compare with.
   * @return True if the the lists contain different contents; false otherwise.
   */
  bool operator!=(const TupleIdList &that) const noexcept { return !(*this == that); }

  /**
   * @return An iterator positioned at the first element in the TID list.
   */
  ConstIterator begin() { return ConstIterator(bit_vector_); }

  /**
   * @return A const iterator position at the first element in the TID list.
   */
  ConstIterator begin() const { return ConstIterator(bit_vector_); }

  /**
   * @return An iterator positioned at the end of the list.
   */
  ConstIterator end() { return ConstIterator(bit_vector_, BitVectorType::kInvalidPos); }

  /**
   * @return A const iterator position at the end of the list.
   */
  ConstIterator end() const { return ConstIterator(bit_vector_, BitVectorType::kInvalidPos); }


  bool IsForConjunction() {
    return is_for_conjunction_;
  }

  bool SetForConjunction(bool is_for_conjunction) {
    return is_for_conjunction;
  }
 private:
  // The validity bit vector
  BitVectorType bit_vector_;
  bool is_for_conjunction_;
};

}  // namespace tpl::sql
