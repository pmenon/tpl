#pragma once

#include <algorithm>
#include <cstring>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "common/common.h"
#include "common/macros.h"

namespace tpl::util {

/**
 * A ChunkedVector is similar to std::vector, but with three important distinctions:
 * (1) ChunkedVector is not templated. Users specify the sizes of the elements and can only request
 *     space to store elements through ChunkedVector::append(). Thus, this class does not manage the
 *     object lifecycle of the elements, only their underlying storage. It will not call object
 *     constructors and destructors.
 * (2) ChunkedVector does not guarantee physical contiguity of all elements, though the majority of
 *     elements are stored contiguously.
 * (3) ChunkedVector ensures that pointers into the contained element are not invalidated through
 *     insertions.
 *
 * ChunkedVectors are composed of a list of fixed-sized memory chunks and one active chunk. Elements
 * <b>within</b> a chunk are stored contiguously, and new elements are inserted into the active
 * chunk (i.e., the most recently allocated chunk and the last chunk in the list of chunks).
 * Appending new elements is an amortized constant O(1) time operation; random access lookups are
 * also constant O(1) time operations. Iteration performance is comparable to std::vector since the
 * majority of elements are contiguous.
 *
 * This class is useful (and usually faster) when you don't need to rely on contiguity of elements,
 * or when you do not know the number of insertions into the vector apriori. In fact, when the
 * number of insertions is unknown, a chunked vector will be roughly 2x faster than a std::vector.
 */
template <typename Allocator = std::allocator<byte>>
class ChunkedVector {
 public:
  // By default, we store 256 elements in each chunk of the vector.
  static constexpr uint32_t kLogNumElementsPerChunk = 8;
  static constexpr uint32_t kNumElementsPerChunk = (1u << kLogNumElementsPerChunk);
  static constexpr uint32_t kChunkPositionMask = kNumElementsPerChunk - 1;

  // Typedefs.
  using allocator_type = Allocator;
  using size_type = std::size_t;
  using chunk_pointer = byte *;
  using chunk_list_allocator_type =
      typename std::allocator_traits<allocator_type>::template rebind_alloc<chunk_pointer>;
  using chunk_list_type = std::vector<chunk_pointer, chunk_list_allocator_type>;
  using chunk_list_iterator = typename chunk_list_type::iterator;
  using chunk_list_const_iterator = typename chunk_list_type::const_iterator;

  /**
   * Construct a chunked vector whose elements have size @em element_size in bytes using the
   * provided allocator.
   * @param element_size The size of each vector element.
   * @param allocator The allocator to use for all chunk allocations.
   */
  ChunkedVector(size_type element_size, const allocator_type &allocator = allocator_type())
      : allocator_(allocator),
        chunks_(chunk_list_allocator_type(allocator_)),
        position_(nullptr),
        end_(nullptr),
        element_size_(element_size),
        num_elements_(0) {
    append_chunk();
  }

  /**
   * Construct a chunked vector whose elements have size @em element_size in bytes using the
   * provided allocator.
   * @param element_size The size of each vector element.
   * @param num_elements The number of elements to initially create.
   * @param allocator The allocator to use for all chunk allocations.
   */
  ChunkedVector(size_type element_size, size_type num_elements,
                const allocator_type &allocator = allocator_type())
      : ChunkedVector(element_size, allocator) {
    resize(num_elements);
  }

  /**
   * Move constructor.
   */
  ChunkedVector(ChunkedVector &&other) noexcept
      : allocator_(std::move(other.allocator_)), chunks_(std::move(other.chunks_)) {
    position_ = other.position_;
    other.position_ = nullptr;

    end_ = other.end_;
    other.end_ = nullptr;

    element_size_ = other.element_size_;
    other.element_size_ = 0;

    num_elements_ = other.num_elements_;
    other.num_elements_ = 0;
  }

  /**
   * Cleanup the vector, releasing all memory back to the allocator.
   */
  ~ChunkedVector() noexcept { deallocate_all_chunks(); }

  /**
   * Move-assign the given vector into this vector.
   * @param other The vector whose contents will be moved into this vector.
   * @return This vector containing the contents previously stored in @em other.
   */
  ChunkedVector &operator=(ChunkedVector &&other) noexcept {
    if (this == &other) {
      return *this;
    }

    // Free anything we're allocate so far
    deallocate_all_chunks();

    allocator_ = std::move(other.allocator_);
    chunks_ = std::move(other.chunks_);

    position_ = other.position_;
    other.position_ = nullptr;

    end_ = other.end_;
    other.end_ = 0;

    element_size_ = other.element_size_;
    other.element_size_ = 0;

    num_elements_ = other.num_elements_;
    other.num_elements_ = 0;

    return *this;
  }

  /**
   * Read-only iterator over the elements in the vector.
   */
  template <typename T>
  class _iterator {
    friend class ChunkedVector;

   public:
    // Iterator typedefs
    using size_type = int64_t;
    using difference_type = int64_t;
    using value_type = T;
    using iterator_category = std::random_access_iterator_tag;
    using pointer = T *;
    using reference = T &;

    _iterator() noexcept
        : curr_(),
          first_(),
          last_(),
          chunk_iter_(),
          chunk_start_(),
          chunk_end_(),
          element_size_() {}

    value_type operator*() noexcept { return curr_; }

    value_type operator*() const noexcept { return curr_; }

    _iterator &operator+=(difference_type offset) {
      // The size (in bytes) of one chunk
      const size_type chunk_size = chunk_alloc_size(element_size_);

      // The total number of bytes between the new and current position
      const difference_type byte_offset =
          offset * static_cast<difference_type>(element_size_) + (curr_ - first_);

      // Offset of the new chunk relative to the current chunk
      difference_type chunk_offset;

      // Optimize for the common case where offset is relatively small. This
      // reduces the number of integer divisions.
      if (byte_offset < chunk_size && byte_offset >= 0) {
        chunk_offset = 0;
      } else if (byte_offset >= chunk_size && byte_offset < 2 * chunk_size) {
        chunk_offset = 1;
      } else if (byte_offset < 0 && byte_offset > (-chunk_size)) {
        chunk_offset = -1;
      } else {
        // When offset is large, division can't be avoided. Force rounding
        // towards negative infinity when the offset is negative.
        chunk_offset = (byte_offset - (offset < 0) * (chunk_size - 1)) / chunk_size;
      }

      // Update the chunk pointer
      set_chunk(chunk_iter_ + chunk_offset);

      // Update the pointer within the new current chunk
      curr_ = first_ + byte_offset - chunk_offset * chunk_size;

      // Finish
      return *this;
    }

    _iterator &operator-=(difference_type offset) {
      *this += (-offset);
      return *this;
    }

    const _iterator operator+(difference_type offset) const {
      _iterator copy(*this);
      copy += offset;
      return copy;
    }

    // Subtraction
    const _iterator operator-(difference_type offset) const {
      _iterator copy(*this);
      copy -= offset;
      return copy;
    }

    _iterator &operator++() noexcept {
      // This is not implemented in terms of operator+=() to optimize for the
      // cases when the offset is known.
      curr_ += element_size_;
      if (curr_ == last_) {
        if (chunk_iter_ + 1 != chunk_end_) {
          set_chunk(chunk_iter_ + 1);
          curr_ = first_;
        }
      }
      return *this;
    }

    const _iterator operator++(int) noexcept {
      _iterator copy(*this);
      ++(*this);
      return copy;
    }

    _iterator &operator--() noexcept {
      // This is not implemented in terms of operator-=() to optimize for the
      // cases when the offset is known.
      if (curr_ == first_) {
        if (chunk_iter_ != chunk_start_) {
          set_chunk(chunk_iter_ - 1);
          curr_ = last_;
        }
      }
      curr_ -= element_size_;
      return *this;
    }

    // Post-decrement
    const _iterator operator--(int) noexcept {
      _iterator copy(*this);
      ++(*this);
      return copy;
    }

    value_type operator[](difference_type idx) const noexcept { return *(this->operator+(idx)); }

    bool operator==(const _iterator &that) const noexcept { return curr_ == that.curr_; }

    bool operator!=(const _iterator &that) const noexcept { return !(*this == that); }

    bool operator<(const _iterator &that) const noexcept {
      if (chunk_iter_ != that.chunk_iter_) {
        return chunk_iter_ < that.chunk_iter_;
      }
      return curr_ < that.curr_;
    }

    bool operator>(const _iterator &that) const noexcept { return that < *this; }

    bool operator<=(const _iterator &that) const noexcept { return !(that < *this); }

    bool operator>=(const _iterator &that) const noexcept { return !(*this < that); }

    difference_type operator-(const _iterator &that) const noexcept {
      const int64_t chunk_size = chunk_alloc_size(element_size_);
      const int64_t elem_size = static_cast<int64_t>(element_size_);

      return ((chunk_iter_ - that.chunk_iter_) * chunk_size +
              ((curr_ - *chunk_iter_) - (that.curr_ - *that.chunk_iter_))) /
             elem_size;
    }

   private:
    _iterator(chunk_list_const_iterator chunk_iter, chunk_list_const_iterator chunk_start,
              chunk_list_const_iterator chunk_finish, byte *position,
              size_type element_size) noexcept
        : curr_(position),
          first_(*chunk_iter),
          last_(*chunk_iter + chunk_alloc_size(element_size)),
          chunk_iter_(chunk_iter),
          chunk_start_(chunk_start),
          chunk_end_(chunk_finish),
          element_size_(element_size) {}

    void set_chunk(chunk_list_const_iterator chunk) {
      chunk_iter_ = chunk;
      first_ = *chunk;
      last_ = first_ + chunk_alloc_size(element_size_);
    }

   protected:
    byte *curr_;
    byte *first_;
    byte *last_;
    chunk_list_const_iterator chunk_iter_;
    chunk_list_const_iterator chunk_start_;
    chunk_list_const_iterator chunk_end_;
    size_type element_size_;
  };

  /**
   * A read-write iterator.
   */
  using iterator = _iterator<byte *>;

  /**
   * A read-only iterator.
   */
  using const_iterator = _iterator<const byte *>;

  /**
   * A read-write reverse iterator.
   */
  using reverse_iterator = std::reverse_iterator<iterator>;

  /**
   * A read-only reverse iterator.
   */
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  /**
   * @return An iterator pointing to the first element in this vector.
   */
  iterator begin() noexcept {
    if (empty()) {
      return iterator();
    }
    return iterator(chunks_.begin(), chunks_.begin(), chunks_.end(), chunks_[0], element_size());
  }

  /**
   * @return An iterator pointing to the first element in this vector.
   */
  const_iterator begin() const noexcept {
    if (empty()) {
      return const_iterator();
    }
    return const_iterator(chunks_.begin(), chunks_.begin(), chunks_.end(), chunks_[0],
                          element_size());
  }

  /**
   * @return An iterator pointing to the first element in this vector.
   */
  const_iterator cbegin() const noexcept {
    if (empty()) {
      return const_iterator();
    }
    return const_iterator(chunks_.begin(), chunks_.begin(), chunks_.end(), chunks_[0],
                          element_size());
  }

  /**
   * @return An iterator pointing to the element following the last in this vector.
   */
  iterator end() noexcept {
    if (empty()) {
      return iterator();
    }
    return iterator(chunks_.end() - 1, chunks_.begin(), chunks_.end(), position_, element_size());
  }

  /**
   * @return An iterator pointing to the element following the last in this vector.
   */
  const_iterator end() const noexcept {
    if (empty()) {
      return const_iterator();
    }
    return const_iterator(chunks_.end() - 1, chunks_.begin(), chunks_.end(), position_,
                          element_size());
  }

  /**
   * @return An iterator pointing to the element following the last in this vector.
   */
  const_iterator cend() const noexcept {
    if (empty()) {
      return const_iterator();
    }
    return const_iterator(chunks_.end() - 1, chunks_.begin(), chunks_.end(), position_,
                          element_size());
  }

  /**
   * @return An reverse iterator to the first element of the reversed vector. Corresponds to the
   *         last element of the non-reversed vector.
   */
  reverse_iterator rbegin() noexcept { return reverse_iterator(end()); }

  /**
   * @return An reverse iterator to the first element of the reversed vector. Corresponds to the
   *         last element of the non-reversed vector.
   */
  const_reverse_iterator rbegin() const noexcept { return const_reverse_iterator(end()); }

  /**
   * @return An iterator positioned one past the last element in the reversed vector.
   */
  reverse_iterator rend() noexcept { return reverse_iterator(begin()); }

  /**
   * @return An iterator positioned one past the last element in the reversed vector.
   */
  const_reverse_iterator rend() const noexcept { return const_reverse_iterator(begin()); }

  /**
   * @return The element at index @em index. This method performs a bounds-check.
   */
  byte *at(size_type idx) {
    if (idx > size()) {
      throw std::out_of_range("Out-of-range access");
    }
    return (*this)[idx];
  }

  /**
   * @return The element at index @em index. This method performs a bounds-check.
   */
  const byte *at(size_type idx) const {
    if (idx > size()) {
      throw std::out_of_range("Out-of-range access");
    }
    return (*this)[idx];
  }

  /**
   * @return The element at index @em index. This method DOES NOT perform a bounds check.
   */
  byte *operator[](size_type idx) noexcept {
    const size_type chunk_idx = chunk_index(idx);
    const size_type chunk_pos = chunk_position(idx);
    return chunks_[chunk_idx] + (element_size() * chunk_pos);
  }

  /**
   * @return The element at index @em index. This method DOES NOT perform a bounds check.
   */
  const byte *operator[](size_type idx) const noexcept {
    const size_type chunk_idx = chunk_index(idx);
    const size_type chunk_pos = chunk_position(idx);
    return chunks_[chunk_idx] + (element_size() * chunk_pos);
  }

  /**
   * @return The first element in the vector. Undefined if the vector is empty.
   */
  byte *front() noexcept {
    TPL_ASSERT(!empty(), "Accessing front() of empty vector");
    return chunks_[0];
  }

  /**
   * @return The first element in the vector. Undefined if the vector is empty.
   */
  const byte *front() const noexcept {
    TPL_ASSERT(!empty(), "Accessing front() of empty vector");
    return chunks_[0];
  }

  /**
   * @return The last element in the vector. Undefined if the vector is empty.
   */
  byte *back() noexcept {
    TPL_ASSERT(!empty(), "Accessing back() of empty vector");
    return this->operator[](size() - 1);
  }

  /**
   * @return The first element in the vector. Undefined if the vector is empty.
   */
  const byte *back() const noexcept {
    TPL_ASSERT(!empty(), "Accessing back() of empty vector");
    return this->operator[](size() - 1);
  }

  /**
   * Make room for a new entry at the end of the vector.
   * @post The returned pointer is guaranteed to be large enough to store one element.
   * @return A pointer to a contiguous memory space where a new entry can be stored.
   */
  byte *append() noexcept {
    if (position_ == end_) {
      append_chunk();
    }

    byte *const result = position_;
    position_ += element_size_;
    num_elements_++;
    return result;
  }

  /**
   * Copy-construct a new element to the end of the vector.
   */
  void push_back(const byte *elem) {
    byte *dest = append();
    std::memcpy(dest, elem, element_size());
  }

  /**
   * Remove the last element from the vector.
   */
  void pop_back() {
    TPL_ASSERT(!empty(), "Popping empty vector");
    if (position_ == chunks_.back()) {
      erase_chunk_at_back();
    }

    position_ -= element_size();
    num_elements_--;
  }

  /**
   * Remove all elements from the vector.
   */
  void clear() {
    deallocate_all_chunks();
    position_ = end_ = nullptr;
    num_elements_ = 0;
  }

  /**
   * Resizes the vector to the specified number of elements. If the number of elements is smaller
   * than the vector's current size the vector is truncated; otherwise, new elements are appended.
   * @param new_size Number of elements the vector should contain.
   */
  void resize(size_type new_size) {
    const size_type len = size();
    if (new_size > len) {
      append_many(new_size - len);
    } else if (new_size < len) {
      erase_many(new_size);
    }
  }

  /**
   * @return True if this vector empty; false otherwise.
   */
  bool empty() const noexcept { return size() == 0; }

  /**
   * @return The number of elements currently in the vector.
   */
  size_type size() const noexcept { return num_elements_; }

  /**
   * @return The size of the elements (in bytes) this vector stores.
   */
  size_type element_size() const noexcept { return element_size_; }

  /**
   * @return The allocator use for all memory allocations made by this vector.
   */
  allocator_type get_allocator() const noexcept { return allocator_type(allocator_); }

  /**
   * @return The size (in bytes) of memory chunks allocated by the vector that make up a chunked
   *         vector, given the size (in bytes) of the elements the vector stores.
   */
  static constexpr size_type chunk_alloc_size(size_type element_size) noexcept {
    return kNumElementsPerChunk * element_size;
  }

 private:
  size_type elements_per_chunk() const noexcept { return kNumElementsPerChunk; }

  size_type chunk_index(size_t idx) const noexcept { return idx >> kLogNumElementsPerChunk; }

  size_type chunk_position(size_type idx) const noexcept { return idx & kChunkPositionMask; }

  [[nodiscard]] byte *allocate_chunk() {
    const size_type alloc_size = chunk_alloc_size(element_size());
    return allocator_.allocate(alloc_size);
  }

  void deallocate_chunk(byte *chunk) {
    const size_type alloc_size = chunk_alloc_size(element_size());
    allocator_.deallocate(chunk, alloc_size);
  }

  // Allocate enough chunks for 'new_elements' elements.
  void allocate_chunks(size_type new_elements) {
    const size_type new_chunks = (new_elements + elements_per_chunk() - 1) / elements_per_chunk();
    chunks_.reserve(chunks_.size() + new_chunks);
    for (size_type i = 0; i < new_chunks; i++) {
      chunks_.push_back(allocate_chunk());
    }
  }

  // Called when position_ == end_ in append().
  void append_chunk() {
    chunks_.push_back(allocate_chunk());
    position_ = chunks_.back();
    end_ = chunks_.back() + chunk_alloc_size(element_size());
  }

  // Called when position_ == chunks_.back() in pop_back();
  void erase_chunk_at_back() {
    TPL_ASSERT(!chunks_.empty(), "No chunks to de-allocate");
    deallocate_chunk(chunks_.back());
    chunks_.pop_back();
    position_ = end_ = chunks_.back() + chunk_alloc_size(element_size());
  }

  // Deallocate all chunks.
  void deallocate_all_chunks() {
    for (auto chunk : chunks_) {
      deallocate_chunk(chunk);
    }
    chunks_.clear();
    position_ = end_ = nullptr;
  }

  void append_many(size_type n) {
    const size_type vacancies = kNumElementsPerChunk - chunk_position(size());
    if (n > vacancies) {
      allocate_chunks(n - vacancies);
      position_ = chunks_.back();
      end_ = chunks_.back() + chunk_alloc_size(element_size());
      // Re-position to last element.
      position_ += element_size() * chunk_position(n - vacancies);
    } else {
      position_ += element_size() * n;
    }
    num_elements_ += n;
  }

  void erase_many(size_type n) {
    TPL_ASSERT(n < size(), "Cannot erase more than current size().");
    const size_type chunk_idx = chunk_index(n);
    for (size_type i = chunk_idx + 1; i < chunks_.size(); i++) {
      deallocate_chunk(chunks_[i]);
    }
    chunks_.resize(chunk_idx + 1);
    position_ = chunks_.back();
    end_ = chunks_.back() + chunk_alloc_size(element_size());
    // Re-position to last element.
    position_ += element_size() * chunk_position(n);
    num_elements_ = n;
  }

 private:
  // The memory allocator we use to acquire memory chunks.
  allocator_type allocator_;
  // The list all chunks.
  chunk_list_type chunks_;
  // The current position in the last chunk and the position of the end.
  byte *position_;
  byte *end_;
  // The size of the elements this vector stores.
  size_type element_size_;
  size_type num_elements_;
};

/**
 * A typed chunked vector.
 */
template <typename T, typename Allocator = std::allocator<T>>
class ChunkedVectorT {
 public:
  using allocator_type = Allocator;
  using value_type = T;
  using size_type = std::size_t;

 private:
  // We need to rebind the templated allocator to one needed by the generic vector.
  using vec_allocator_type = typename std::allocator_traits<Allocator>::template rebind_alloc<byte>;
  using vec_type = ChunkedVector<vec_allocator_type>;

 public:
  static constexpr size_type kNumChunkElements = vec_type::kNumElementsPerChunk;

  /**
   * Construct a vector using the given allocator.
   */
  ChunkedVectorT(allocator_type allocator = {}) : vec_(sizeof(T), vec_allocator_type(allocator)) {}

  /**
   * Move constructor.
   * @param that The vector to move into this instance.
   */
  ChunkedVectorT(ChunkedVectorT &&that) : vec_(std::move(that.vec_)) {}

  /**
   * Copy not supported yet.
   */
  DISALLOW_COPY(ChunkedVectorT);

  /**
   * Destructor.
   */
  ~ChunkedVectorT() { std::destroy(begin(), end()); }

  /**
   * Move assignment.
   * @param that The vector to move into this.
   * @return This vector instance.
   */
  ChunkedVectorT &operator=(ChunkedVectorT &&that) noexcept {
    std::swap(vec_, that.vec_);
    return *this;
  }

  /**
   * Generic iterator over a typed chunked vector.
   */
  template <typename U, typename BaseIterator>
  class _iterator {
    friend class ChunkedVectorT;

   public:
    using size_type = typename BaseIterator::size_type;
    using difference_type = typename BaseIterator::difference_type;
    using value_type = U;
    using iterator_category = std::random_access_iterator_tag;
    using pointer = U *;
    using reference = U &;

    _iterator() : iter_() {}

    reference operator*() const noexcept { return *reinterpret_cast<pointer>(*iter_); }

    _iterator &operator+=(difference_type offset) noexcept {
      iter_ += offset;
      return *this;
    }

    _iterator &operator-=(difference_type offset) noexcept {
      iter_ -= offset;
      return *this;
    }

    const _iterator operator+(difference_type offset) const noexcept {
      return _iterator(iter_ + offset);
    }

    const _iterator operator-(difference_type offset) const noexcept {
      return _iterator(iter_ - offset);
    }

    _iterator &operator++() noexcept {
      ++iter_;
      return *this;
    }

    const _iterator operator++(int) noexcept { return _iterator(iter_++); }

    _iterator &operator--() noexcept {
      --iter_;
      return *this;
    }

    const _iterator operator--(int) noexcept { return _iterator(iter_--); }

    reference operator[](int64_t idx) const noexcept { return *reinterpret_cast<T *>(iter_[idx]); }

    bool operator==(const _iterator &that) const { return iter_ == that.iter_; }

    bool operator!=(const _iterator &that) const { return iter_ != that.iter_; }

    bool operator<(const _iterator &that) const { return iter_ < that.iter_; }

    bool operator<=(const _iterator &that) const { return iter_ <= that.iter_; }

    bool operator>(const _iterator &that) const { return iter_ > that.iter_; }

    bool operator>=(const _iterator &that) const { return iter_ >= that.iter_; }

    difference_type operator-(const _iterator &that) const { return iter_ - that.iter_; }

   private:
    _iterator(BaseIterator iter) : iter_(iter) {}

   private:
    BaseIterator iter_;
  };

  /**
   * A read-write iterator.
   */
  using iterator = _iterator<T, typename vec_type::iterator>;

  /**
   * A read-only iterator.
   */
  using const_iterator = _iterator<const T, typename vec_type::const_iterator>;

  /**
   * A read-write reverse iterator.
   */
  using reverse_iterator = std::reverse_iterator<iterator>;

  /**
   * A read-only reverse iterator.
   */
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  /**
   * @return A read-write random access iterator that points to the first element in the vector.
   */
  iterator begin() { return iterator(vec_.begin()); }

  /**
   * @return A read-write random access iterator that points to the first element in the vector.
   */
  const_iterator begin() const { return const_iterator(vec_.begin()); }

  /**
   * @return A read-write random access iterator that points to the element after the last in the
   *         vector.
   */
  iterator end() { return iterator(vec_.end()); }

  /**
   * @return A read-write random access iterator that points to the element after the last in the
   *         vector.
   */
  const_iterator end() const { return const_iterator(vec_.end()); }

  /**
   * @return An reverse iterator to the first element of the reversed vector. Corresponds to the
   *         last element of the non-reversed vector.
   */
  reverse_iterator rbegin() noexcept { return reverse_iterator(end()); }

  /**
   * @return An reverse iterator to the first element of the reversed vector. Corresponds to the
   *         last element of the non-reversed vector.
   */
  const_reverse_iterator rbegin() const noexcept { return const_reverse_iterator(end()); }

  /**
   * @return An iterator positioned one past the last element in the reversed vector.
   */
  reverse_iterator rend() noexcept { return reverse_iterator(begin()); }

  /**
   * @return An iterator positioned one past the last element in the reversed vector.
   */
  const_reverse_iterator rend() const noexcept { return const_reverse_iterator(begin()); }

  // -------------------------------------------------------
  // Element access
  // -------------------------------------------------------

  /**
   * @return A mutable reference to the element at index @em idx, skipping any bounds check.
   */
  T &operator[](size_type idx) noexcept { return *reinterpret_cast<T *>(vec_[idx]); }

  /**
   * @return A mutable reference to the element at index @em idx, skipping any bounds check.
   */
  const T &operator[](size_type idx) const noexcept {
    return *reinterpret_cast<const T *>(vec_[idx]);
  }

  /**
   * @return A mutable reference to the first element in this vector. Has undefined behavior when
   *         accessing an empty vector.
   */
  T &front() noexcept { return *reinterpret_cast<T *>(vec_.front()); }

  /**
   * @return An immutable reference to the first element in this vector. Has undefined behavior when
   *         accessing an empty vector.
   */
  const T &front() const noexcept { return *reinterpret_cast<const T *>(vec_.front()); }

  /**
   * @return A mutable reference to the last element in the vector. Has undefined behavior when
   *         accessing an empty vector.
   */
  T &back() noexcept { return *reinterpret_cast<T *>(vec_.back()); }

  /**
   * @return An immutable reference to the last element in the vector. Has undefined behavior when
   *         accessing an empty vector.
   */
  const T &back() const noexcept { return *reinterpret_cast<const T *>(vec_.back()); }

  /**
   * Clear all elements from the vector.
   */
  void clear() {
    std::destroy(begin(), end());
    vec_.clear();
  }

  // -------------------------------------------------------
  // Size/Capacity
  // -------------------------------------------------------

  /**
   * @return True if the vector is empty; false otherwise.
   */
  bool empty() const noexcept { return vec_.empty(); }

  /**
   * @return The number of elements in this vector.
   */
  size_type size() const noexcept { return vec_.size(); }

  /**
   * Resizes the vector to the specified number of elements. If the number of elements is smaller
   * than the vector's current size the vector is truncated; otherwise, new elements are appended.
   * @param new_size Number of elements the vector should contain.
   */
  void resize(size_type n) { vec_.resize(n); }

  // -------------------------------------------------------
  // Modifiers
  // -------------------------------------------------------

  /**
   * In-place construct an element using arguments @em args and append to the end of the vector.
   */
  template <class... Args>
  void emplace_back(Args &&...args) {
    T *space = reinterpret_cast<T *>(vec_.append());
    new (space) T(std::forward<Args>(args)...);
  }

  /**
   * Copy construct the provided element @em to the end of the vector.
   */
  void push_back(const T &elem) {
    T *space = reinterpret_cast<T *>(vec_.append());
    new (space) T(elem);
  }

  /**
   * Move-construct the provided element @em to the end of the vector.
   */
  void push_back(T &&elem) {
    T *space = reinterpret_cast<T *>(vec_.append());
    new (space) T(std::move(elem));
  }

  /**
   * Remove the last element from the vector. Undefined if the vector is empty.
   */
  void pop_back() {
    TPL_ASSERT(!empty(), "Popping from an empty vector");
    T &removed = back();
    vec_.pop_back();
    std::destroy_at(&removed);
  }

 private:
  vec_type vec_;  // The generic backing vector.
};

}  // namespace tpl::util
