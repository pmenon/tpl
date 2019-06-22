#pragma once

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
  FixedLengthBuffer() : index_(0) {}

  /**
   * Append an element to the end of the buffer. This operation is unsafe if the
   * buffer is fool, i.e., it's the caller's responsibility to check there is
   * sufficient room for a new element because no error will be thrown
   * otherwise.
   * @param val The value to append to the buffer.
   */
  void append(const T val) { buf_[index_++] = val; }

  /**
   * Clear elements from the buffer, resetting the size to zero.
   */
  void clear() { index_ = 0; }

  /**
   * Return the number of elements currently in the buffer.
   */
  u32 size() const noexcept { return index_; }

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
  const T &operator[](const u32 idx) const noexcept {
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
  T &operator[](const u32 idx) noexcept {
    TPL_ASSERT(idx < size(), "Out-of-bounds buffer access");
    return buf_[idx];
  }

  /**
   * Access an element in this buffer with bounds checking.
   * @param idx The index of the element to access.
   * @return A const reference to the element in this buffer at a given index.
   */
  const T &at(const u32 idx) const {
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
  T &at(const u32 idx) {
    if (idx >= size()) {
      throw std::out_of_range("Out-of-range access");
    }
    return (*this)[idx];
  }

  // -------------------------------------------------------
  // Iterators
  // -------------------------------------------------------

  T *begin() { return buf_.begin(); }
  T *end() { return begin() + size(); }
  const T *begin() const { return buf_.begin(); }
  const T *end() const { return begin() + size(); }

 private:
  // The backing container.
  std::array<T, N> buf_;
  // The index where the next element is inserted.
  u32 index_;
};

}  // namespace tpl::util
