#pragma once

#include <iosfwd>
#include <string>
#include <type_traits>

#include "common/macros.h"
#include "util/bit_vector.h"

namespace tpl::sql {

/**
 * An ordered set of tuple IDs used during query execution to efficiently represent valid tuples in
 * a vector projection. A TupleIdList can store TIDs in the range [0, kDefaultVectorSize] (either
 * 1024 or 2048), typical during vector processing.
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
  using BitVectorT = util::BitVector<uint64_t>;

  explicit TupleIdList(uint32_t size = kDefaultVectorSize) : bit_vector_(size) {}

  /**
   * Is the tuple with the given ID in this list?
   * @param tid The tuple ID to check.
   * @return True if the tuple is in the list; false otherwise.
   */
  bool Contains(const uint32_t tid) const { return bit_vector_.Test(tid); }

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
   * Add all tuples whose IDs are in the range [start_tid, end_tid). Note the
   * half-open interval!
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
   * Either enable or disable the tuple with the given ID depending on the
   * value of @em enable. If @em enable is true, the tuple is added to the list,
   * and otherwise it is disabled.
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
   * Intersect the set of tuple IDs in this list with the tuple IDs in the
   * provided list, modifying this list in-place.
   * @param other The list to intersect with.
   */
  void IntersectWith(const TupleIdList &other) { bit_vector_.Intersect(other.bit_vector_); }

  /**
   * Union the set of tuple IDs in this list with the tuple IDs in the provided
   * list, modifying this list in-place.
   * @param other The list to union with.
   */
  void UnionWith(const TupleIdList &other) { bit_vector_.Union(other.bit_vector_); }

  /**
   * Remove all tuple IDs from this list that are also present in the provided
   * list, modifying this list in-place.
   * @param other The list to unset from.
   */
  void UnsetFrom(const TupleIdList &other) { bit_vector_.Difference(other.bit_vector_); }

  /**
   * Build a list of tuple IDs as a subset of the IDs in the input list
   * @em input for which the provided function @em f returns true.
   * @tparam F A functor that accepts a 32-bit tuple ID and returns a boolean.
   * @param input The input list to read from.
   * @param f The function that filters the IDs from the input, returning true
   *          for valid tuples, and false otherwise.
   */
  template <typename F>
  void BuildFromOtherList(const TupleIdList &input, F &&f) {
    static_assert(std::is_invocable_r_v<bool, F, uint32_t>,
                  "List construction callback must a single-argument functor "
                  "accepting an unsigned 32-bit index and returning a boolean "
                  "indicating if the given TID is considered 'valid' and "
                  "should be included in the output list");

    for (BitVectorT::WordType i = 0; i < input.bit_vector_.num_words(); i++) {
      const auto start = i * BitVectorT::kWordSizeBits;
      BitVectorT::WordType word = input.bit_vector_.GetWord(i);
      BitVectorT::WordType word_result = 0;
      while (word != 0) {
        const auto t = word & -word;
        const auto r = util::BitUtil::CountTrailingZeros(word);
        const auto tid = start + r;
        const auto cond = static_cast<BitVectorT::WordType>(f(tid));
        word_result |= (cond << r);
        word ^= t;
      }
      bit_vector_.SetWord(i, word_result);
    }
  }

  /**
   * Build a list of tuple IDs as a subset of all TIDs this list can support
   * (i.e., in the range [0, kDefaultVectorSize]) for which the provided
   * function @em f returns true.
   * @tparam F A functor that accepts a 32-bit tuple ID and returns a boolean.
   * @param input The input list to read from.
   * @param f The function that filters the IDs from the input, returning true
   *          for valid tuples, and false otherwise.
   */
  template <typename F>
  void BuildFromFunction(F &&f) {
    static_assert(std::is_invocable_r_v<bool, F, uint32_t>,
                  "List construction callback must a single-argument functor "
                  "accepting an unsigned 32-bit index and returning a boolean "
                  "indicating if the given TID is considered 'valid' and "
                  "should be included in the output list");

    // Process batches of 64 at-a-time. This loop should be fully vectorized for
    // fundamental types.
    for (BitVectorT::WordType i = 0; i < bit_vector_.num_words(); i++) {
      const auto start = i * BitVectorT::kWordSizeBits;
      BitVectorT::WordType word_result = 0;
      for (BitVectorT::WordType j = 0; j < BitVectorT::kWordSizeBits; j++) {
        const auto cond = static_cast<BitVectorT::WordType>(f(start + j));
        word_result |= (cond << j);
      }
      bit_vector_.SetWord(i, word_result);
    }

    // Scalar tail
    for (BitVectorT::WordType i = bit_vector_.num_words() * BitVectorT::kWordSizeBits;
         i < kDefaultVectorSize; i++) {
      bit_vector_.Set(i, f(i));
    }
  }

  /**
   * Convert the given selection match vector to a TID list. The match vector is
   * assumed to be boolean-like array, but with saturated values. This means
   * that the value 'true' or 1 is physically encoded as all-1s, i.e., a true
   * value is the 8-byte value 255 = 11111111b, and the value 'false' or 0 is
   * encoded as all zeros. This is typically used during selections which
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
   * Return the number of active tuples in the list.
   * @return The number of active tuples in the list.
   */
  uint32_t GetTupleCount() const { return bit_vector_.CountOnes(); }

  /**
   * Return the capacity of the list.
   * @return The capacity of the TID list.
   */
  uint32_t GetListCapacity() const { return bit_vector_.num_bits(); }

  /**
   * Return the selectivity of the list as a fraction in the range [0.0, 1.0].
   * @return The selectivity of the list, i.e., the fraction of the tuples that
   *         are considered "active".
   */
  float ComputeSelectivity() const {
    return static_cast<float>(GetTupleCount()) / GetListCapacity();
  }

  /**
   * Convert this tuple ID list into a dense selection index vector.
   * @param[out] sel_vec The output selection vector.
   * @return The number of elements in the generated selection vector.
   */
  uint32_t AsSelectionVector(uint16_t *sel_vec) const;

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
   * Return a string representation of this vector.
   */
  std::string ToString() const;

  /**
   * Print a string representation of this vector to the output stream.
   */
  void Dump(std::ostream &stream) const;

  /**
   * Access the internal bit vector.
   */
  BitVectorT *GetMutableBits() { return &bit_vector_; }

 private:
  // The validity bit vector
  BitVectorT bit_vector_;
};

}  // namespace tpl::sql
