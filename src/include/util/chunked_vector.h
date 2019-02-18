#pragma once

#include "llvm/ADT/SmallVector.h"

#include "util/common.h"
#include "util/region.h"

namespace tpl::util {

class ChunkedVectorIterator;

/// A ChunkedVector is similar to a std::vector, but it is untyped, it does not
/// guarantee physical contiguity of all elements in the vector, and it ensures
/// that pointers into the container are not invalidated through insertions.
///
/// Internally, it maintains an array of fixed-sized memory chunks. Elements
/// inside a chunk are stored contiguously. Appending (i.e., push_back()) is an
/// O(1) constant-time operation and iteration performance is comparable to
/// std::vector. As memory is used, fixed-sized memory allocations are made
/// against the injected memory allocator to store additional elements.
///
/// This class is useful (and usually faster) when you don't need to rely on
/// contiguity of elements, or when you do not know the number of insertions
/// into the vector apriori. In fact, when the number of insertions is unknown,
/// a chunked vector will be roughly 2x faster than a std::vector.
class ChunkedVector {
 public:
  // We store 256 elements in each chunk of the vector
  static constexpr const u32 kLogNumElementsPerChunk = 8;
  static constexpr const u32 kNumElementsPerChunk =
      (1u << kLogNumElementsPerChunk);
  static constexpr const u32 kChunkPositionMask = kNumElementsPerChunk - 1;

  /// Constructor
  ChunkedVector(util::Region *region, std::size_t element_size) noexcept;

  // -------------------------------------------------------
  // Iterators
  // -------------------------------------------------------

  ChunkedVectorIterator Begin();
  ChunkedVectorIterator End();

  // -------------------------------------------------------
  // Element access
  // -------------------------------------------------------

  /// Return a pointer to the entry at the given index
  /// NOTE: This is an unchecked index lookup
  byte *EntryAt(std::size_t idx);
  const byte *EntryAt(std::size_t idx) const;

  /// Operator overloaded vector access
  /// NOTE: This is an unchecked index lookup
  byte *operator[](std::size_t idx) { return EntryAt(idx); }
  const byte *operator[](std::size_t idx) const { return EntryAt(idx); }

  // -------------------------------------------------------
  // Modification
  // -------------------------------------------------------

  /// Append a new entry at the end of the vector, returning a contiguous memory
  /// space where the element can be written to by the caller
  byte *Append();

  // -------------------------------------------------------
  // Size/Capacity
  // -------------------------------------------------------

  /// Is this vector empty?
  bool Empty() const noexcept { return chunks_.empty(); }

  /// Return the number of elements currently stored in the vector
  std::size_t Size() const noexcept;

  /// Given the size (in bytes) of an individual element, compute the size of
  /// each chunk in the chunked vector
  static std::size_t ChunkAllocSize(std::size_t element_size) {
    return kNumElementsPerChunk * element_size;
  }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  util::Region *region() { return region_; }

  std::size_t element_size() const noexcept { return element_size_; }

 private:
  // Allocate a new chunk
  void AllocateChunk();

 private:
  // The memory allocator we use to acquire memory chunks
  util::Region *region_;

  // The list of pointers to all chunks
  llvm::SmallVector<byte *, 4> chunks_;

  // The current position in the last chunk and the position of the end
  byte *position_;
  byte *end_;

  // The size of the elements this vector stores
  std::size_t element_size_;
};

// ---------------------------------------------------------
// GenericChunkedVector Iterator
// ---------------------------------------------------------

/// An iterator over the elements in a generic chunked-vector
class ChunkedVectorIterator {
 public:
  ChunkedVectorIterator() noexcept
      : chunks_iter_(), element_size_(0), curr_(nullptr), end_(nullptr) {}

  ChunkedVectorIterator(llvm::SmallVectorImpl<byte *>::iterator chunks_iter,
                        byte *position, std::size_t element_size) noexcept
      : chunks_iter_(chunks_iter),
        element_size_(element_size),
        curr_(position),
        end_(*chunks_iter + ChunkedVector::ChunkAllocSize(element_size)) {}

  byte *operator*() const noexcept { return curr_; }

  // Pre-increment
  ChunkedVectorIterator &operator++() noexcept {
    curr_ += element_size_;
    if (curr_ == end_) {
      byte *start = *++chunks_iter_;
      curr_ = start;
      end_ = start + ChunkedVector::ChunkAllocSize(element_size_);
    }
    return *this;
  }

  bool operator==(const ChunkedVectorIterator &that) const noexcept {
    return curr_ == that.curr_;
  }

  bool operator!=(const ChunkedVectorIterator &that) const noexcept {
    return !(this->operator==(that));
  }

 private:
  llvm::SmallVectorImpl<byte *>::iterator chunks_iter_;
  std::size_t element_size_;
  byte *curr_;
  byte *end_;
};

// ---------------------------------------------------------
// ChunkedVector implementation
// ---------------------------------------------------------

inline ChunkedVector::ChunkedVector(util::Region *region,
                                    std::size_t element_size) noexcept
    : region_(region),
      position_(nullptr),
      end_(nullptr),
      element_size_(element_size) {}

inline ChunkedVectorIterator ChunkedVector::Begin() {
  if (Empty()) {
    return ChunkedVectorIterator();
  }

  return ChunkedVectorIterator(chunks_.begin(), chunks_.front(),
                               element_size());
}

inline ChunkedVectorIterator ChunkedVector::End() {
  if (Empty()) {
    return ChunkedVectorIterator();
  }

  return ChunkedVectorIterator(chunks_.end() - 1, position_, element_size());
}

inline byte *ChunkedVector::EntryAt(const std::size_t idx) {
  std::size_t chunk_idx = idx >> kLogNumElementsPerChunk;
  std::size_t chunk_pos = idx & kChunkPositionMask;
  return chunks_[chunk_idx] + (element_size() * chunk_pos);
}

inline const byte *ChunkedVector::EntryAt(const std::size_t idx) const {
  std::size_t chunk_idx = idx >> kLogNumElementsPerChunk;
  std::size_t chunk_pos = idx & kChunkPositionMask;
  return chunks_[chunk_idx] + (element_size() * chunk_pos);
}

inline byte *ChunkedVector::Append() {
  if (position_ == end_) {
    AllocateChunk();
  }

  byte *const result = position_;
  position_ += element_size_;
  return result;
}

inline void ChunkedVector::AllocateChunk() {
  std::size_t alloc_size = ChunkAllocSize(element_size());
  byte *new_chunk = static_cast<byte *>(region()->Allocate(alloc_size));
  chunks_.push_back(new_chunk);
  position_ = new_chunk;
  end_ = new_chunk + alloc_size;
}

inline std::size_t ChunkedVector::Size() const noexcept {
  if (Empty()) {
    return 0;
  }

  std::size_t full_chunk_elems = kNumElementsPerChunk * (chunks_.size() - 1);
  std::size_t partial_chunk_elems =
      (position_ - chunks_.back()) / element_size();

  return full_chunk_elems + partial_chunk_elems;
}

}  // namespace tpl::util