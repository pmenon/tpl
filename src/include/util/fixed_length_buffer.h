#pragma once

#include <iterator>

namespace tpl::util {

/**
 * A fixed-length buffer is a container that stores elements of a given type in
 * a contiguous memory space, whose maximum capacity is a compile-time type, but
 * whose runtime size may be less than its capacity.
 *
 * It can be viewed as a vector with pre-allocated space and fixed capacity(),
 * or as a wrapper around a vanilla C/C++ array, but whose size may change over
 * time.
 * @tparam T The type of the elements in the container.
 * @tparam N The maximum number of elements in the buffer.
 */
template <typename T, u32 N>
class FixedLengthBuffer {
 public:
  using value_type = T;
  using pointer = value_type *;
  using const_pointer = const value_type *;
  using reference = value_type &;
  using const_reference = const value_type &;
  using iterator = value_type *;
  using const_iterator = const value_type *;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  /**
   * Create an empty buffer.
   */
  FixedLengthBuffer() : index_(0) {}

  /**
   * Append an element to the end of the buffer. This operation is unsafe if the
   * buffer is full. In other words, it's the caller's responsibility to check
   * there is sufficient room for a new element because no error will be thrown
   * otherwise.
   * @param val The value to append to the buffer.
   */
  void append(const value_type &val) { buf_[index_++] = val; }

  /**
   * Clear elements from the buffer, resetting the size to zero.
   */
  void clear() { index_ = 0; }

  /**
   * Return the number of elements currently in the buffer.
   */
  size_type size() const noexcept { return index_; }

  /**
   * Maximum capacity of this buffer.
   */
  size_type capacity() const noexcept { return N; }

  /**
   * Is this buffer empty?
   */
  bool empty() const noexcept { return index_ == 0; }

  /**
   * Array access operator. This method doesn't perform bounds checking. Use
   * @em At() if you want automatic bounds-checked array access.
   * @see FixedLengthBuffer::At()
   * @param idx The index of the element to access.
   * @return A const reference to the element in this buffer at a given index.
   */
  constexpr const_reference operator[](const u32 idx) const noexcept {
    TPL_ASSERT(idx < size(), "Out-of-bounds buffer access");
    return buf_[idx];
  }

  /**
   * Array access operator. This method doesn't perform bounds checking. Use
   * @em At() if you want automatic bounds-checked array access.
   * @see FixedLengthBuffer::At()
   * @param idx The index of the element to access.
   * @return A const reference to the element in this buffer at a given index.
   */
  constexpr reference operator[](const u32 idx) noexcept {
    TPL_ASSERT(idx < size(), "Out-of-bounds buffer access");
    return buf_[idx];
  }

  /**
   * Access an element in this buffer with bounds checking.
   * @param idx The index of the element to access.
   * @return A const reference to the element in this buffer at a given index.
   */
  constexpr const_reference at(const u32 idx) const {
    if (idx >= size()) {
      throw std::out_of_range("Out-of-range access");
    }
    return (*this)[idx];
  }

  /**
   * Access an element in this buffer with bounds checking.
   * @param idx The index of the element to access.
   * @return A reference to the element in this buffer at a given index.
   */
  constexpr reference at(const u32 idx) {
    if (idx >= size()) {
      throw std::out_of_range("Out-of-range access");
    }
    return (*this)[idx];
  }

  // -------------------------------------------------------
  // Iterators
  // -------------------------------------------------------

  constexpr iterator begin() { return buf_.begin(); }
  constexpr iterator end() { return begin() + size(); }
  constexpr const_iterator begin() const { return buf_.begin(); }
  constexpr const_iterator end() const { return begin() + size(); }
  constexpr reverse_iterator rbegin() { return reverse_iterator(end()); }
  constexpr reverse_iterator rend() { return reverse_iterator(begin()); }
  constexpr reverse_iterator crbegin() { return const_reverse_iterator(end()); }
  constexpr reverse_iterator crend() { return const_reverse_iterator(begin()); }

 private:
  // The backing container.
  std::array<T, N> buf_;
  // The index where the next element is inserted.
  u32 index_;
};

}  // namespace tpl::util
