#pragma once

#include <array>
#include <iterator>

#include "common/macros.h"

namespace tpl::util {

/**
 * A fixed-length buffer is a container that stores elements of a given type in a contiguous memory
 * space, whose maximum capacity is a compile-time type, but whose runtime size may be less than its
 * capacity.
 *
 * It can be viewed as a vector with pre-allocated space and fixed capacity(), or as a wrapper
 * around a vanilla C/C++ array, but whose size may change over time.
 *
 * @tparam T The type of the elements in the container.
 * @tparam N The maximum number of elements in the buffer.
 */
template <typename T, uint32_t N>
class FixedLengthBuffer {
 public:
  using ValueType = T;
  using Pointer = ValueType *;
  using ConstPointer = const ValueType *;
  using Reference = ValueType &;
  using ConstReference = const ValueType &;
  using Iterator = ValueType *;
  using ConstIterator = const ValueType *;
  using ReverseIterator = std::reverse_iterator<Iterator>;
  using ConstReverseIterator = std::reverse_iterator<ConstIterator>;
  using SizeType = std::size_t;
  using DifferenceType = std::ptrdiff_t;

  /**
   * Create an empty buffer.
   */
  FixedLengthBuffer() : index_(0) {}

  /**
   * Append an element to the end of the buffer. The element is copied to the back of the buffer.
   * This operation is unsafe if the buffer is full; it's the caller's responsibility to check there
   * is sufficient room for a new element because no error will be thrown otherwise.
   * @param val The value to append to the buffer.
   */
  void Append(const ValueType &val) { buf_[index_++] = val; }

  /**
   * Append an element to the end of the buffer. The element is moved to the back of the buffer.
   * This operation is unsafe if the buffer is full; it's the caller's responsibility to check there
   * is sufficient room for a new element because no error will be thrown otherwise.
   * @param val The value to append to the buffer.
   */
  void Append(ValueType &&val) { buf_[index_++] = std::move(val); }

  /**
   * Clear elements from the buffer, resetting the size to zero.
   */
  void Clear() { index_ = 0; }

  /**
   * Return the number of elements currently in the buffer.
   */
  SizeType GetSize() const noexcept { return index_; }

  /**
   * Maximum capacity of this buffer.
   */
  SizeType GetCapacity() const noexcept { return N; }

  /**
   * @return True if the buffer is empty; false otherwise.
   */
  bool IsEmpty() const noexcept { return index_ == 0; }

  /**
   * Array access operator. This method does not perform a bounds check. Use FixedLengthBuffer::At()
   * if you want automatic bounds-checked array access.
   *
   * @see FixedLengthBuffer::At()
   *
   * @param idx The index of the element to access.
   * @return A const reference to the element in this buffer at a given index.
   */
  constexpr ConstReference operator[](const uint32_t idx) const noexcept {
    TPL_ASSERT(idx < GetSize(), "Out-of-bounds buffer access");
    return buf_[idx];
  }

  /**
   * Array access operator. This method does not perform a bounds check. Use FixedLengthBuffer::At()
   * if you want automatic bounds-checked array access.
   *
   * @see FixedLengthBuffer::At()
   *
   * @param idx The index of the element to access.
   * @return A const reference to the element in this buffer at a given index.
   */
  constexpr Reference operator[](const uint32_t idx) noexcept {
    TPL_ASSERT(idx < GetSize(), "Out-of-bounds buffer access");
    return buf_[idx];
  }

  /**
   * Access an element in this buffer with bounds checking.
   * @param idx The index of the element to access.
   * @return A const reference to the element in this buffer at a given index.
   */
  constexpr ConstReference At(const uint32_t idx) const {
    if (idx >= GetSize()) {
      throw std::out_of_range("Out-of-range access");
    }
    return (*this)[idx];
  }

  /**
   * Access an element in this buffer with bounds checking.
   * @param idx The index of the element to access.
   * @return A reference to the element in this buffer at a given index.
   */
  constexpr Reference At(const uint32_t idx) {
    if (idx >= GetSize()) {
      throw std::out_of_range("Out-of-range access");
    }
    return (*this)[idx];
  }

  constexpr Iterator begin() { return buf_.begin(); }
  constexpr Iterator end() { return begin() + GetSize(); }
  constexpr ConstIterator begin() const { return buf_.begin(); }
  constexpr ConstIterator end() const { return begin() + GetSize(); }
  constexpr ReverseIterator rbegin() { return ReverseIterator(end()); }
  constexpr ReverseIterator rend() { return ReverseIterator(begin()); }
  constexpr ReverseIterator crbegin() { return ConstReverseIterator(end()); }
  constexpr ReverseIterator crend() { return ConstReverseIterator(begin()); }

 private:
  // The backing container.
  std::array<T, N> buf_;

  // The index where the next element is inserted.
  uint32_t index_;
};

}  // namespace tpl::util
