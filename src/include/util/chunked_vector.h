#pragma once

#include "llvm/ADT/SmallVector.h"

#include "util/common.h"
#include "util/region.h"

namespace tpl::util {

class ChunkedVectorIterator;

/// A ChunkedVector is similar to STL's std::vector, but with three important
/// distinctions: ChunkedVectors are untyped and are not templated;
/// ChunkedVectors do not guarantee physical contiguity of all elements, though
/// the majority of elements are stored contiguously; ChunkedVectors ensures
/// that pointers into the container are not invalidated through insertions.
///
/// ChunkedVectors are composed of a list of fixed-sized memory chunks and one
/// active chunk. Elements \a within a chunk are stored contiguously, and new
/// elements are inserted into the active chunk (i.e., the most recently
/// allocated chunk and the last chunk in the list of chunks). Appending new
/// elements is an amortized constant O(1) time operation; random access lookups
/// are also constant O(1) time operations. Iteration performance is comparable
/// to std::vector since the majority of elements are contiguous.
///
/// This class is useful (and usually faster) when you don't need to rely on
/// contiguity of elements, or when you do not know the number of insertions
/// into the vector apriori. In fact, when the number of insertions is unknown,
/// a chunked vector will be roughly 2x faster than a std::vector.
class ChunkedVector {
 public:
  // clang-format off
  // We store 256 elements in each chunk of the vector
  static constexpr const u32 kLogNumElementsPerChunk = 8;
  static constexpr const u32 kNumElementsPerChunk = (1u << kLogNumElementsPerChunk);
  static constexpr const u32 kChunkPositionMask = kNumElementsPerChunk - 1;
  // clang-format on

  ChunkedVector(util::Region *region, std::size_t element_size) noexcept;
  ~ChunkedVector() noexcept;

  // -------------------------------------------------------
  // Iterators
  // -------------------------------------------------------

  ChunkedVectorIterator begin() noexcept;
  ChunkedVectorIterator end() noexcept;

  // -------------------------------------------------------
  // Element access
  // -------------------------------------------------------

  /// Checked indexed lookup
  byte *at(std::size_t idx);
  const byte *at(std::size_t idx) const;

  /// Unchecked indexed lookup
  byte *operator[](std::size_t idx) noexcept;
  const byte *operator[](std::size_t idx) const noexcept;

  // -------------------------------------------------------
  // Modification
  // -------------------------------------------------------

  /// Append a new entry at the end of the vector, returning a contiguous memory
  /// space where the element can be written to by the caller
  byte *append() noexcept;

  // -------------------------------------------------------
  // Size/Capacity
  // -------------------------------------------------------

  /// Is this vector empty?
  bool empty() const noexcept { return size() == 0; }

  /// Return the number of elements in the chunked vector
  std::size_t size() const noexcept { return num_elements_; }

  /// Given the size (in bytes) of an individual element, compute the size of
  /// each chunk in the chunked vector
  static constexpr std::size_t ChunkAllocSize(std::size_t element_size) {
    return kNumElementsPerChunk * element_size;
  }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

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
  std::size_t num_elements_;
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
      element_size_(element_size),
      num_elements_(0) {}

inline ChunkedVector::~ChunkedVector() noexcept {
  const std::size_t chunk_size = ChunkAllocSize(element_size());
  for (auto *chunk : chunks_) {
    region_->Deallocate(chunk, chunk_size);
  }
}

inline ChunkedVectorIterator ChunkedVector::begin() noexcept {
  if (empty()) {
    return ChunkedVectorIterator();
  }
  return ChunkedVectorIterator(chunks_.begin(), chunks_[0], element_size());
}

inline ChunkedVectorIterator ChunkedVector::end() noexcept {
  if (empty()) {
    return ChunkedVectorIterator();
  }
  return ChunkedVectorIterator(chunks_.end() - 1, position_, element_size());
}

inline byte *ChunkedVector::at(size_t idx) {
  if (idx > size()) {
    throw std::out_of_range("Out-of-range access");
  }
  return (*this)[idx];
}

inline const byte *ChunkedVector::at(size_t idx) const {
  if (idx > size()) {
    throw std::out_of_range("Out-of-range access");
  }
  return (*this)[idx];
}

inline byte *ChunkedVector::operator[](std::size_t idx) noexcept {
  const std::size_t chunk_idx = idx >> kLogNumElementsPerChunk;
  const std::size_t chunk_pos = idx & kChunkPositionMask;
  return chunks_[chunk_idx] + (element_size() * chunk_pos);
}

inline const byte *ChunkedVector::operator[](std::size_t idx) const noexcept {
  const std::size_t chunk_idx = idx >> kLogNumElementsPerChunk;
  const std::size_t chunk_pos = idx & kChunkPositionMask;
  return chunks_[chunk_idx] + (element_size() * chunk_pos);
}

inline void ChunkedVector::AllocateChunk() {
  std::size_t alloc_size = ChunkAllocSize(element_size());
  byte *new_chunk = static_cast<byte *>(region_->Allocate(alloc_size));
  chunks_.push_back(new_chunk);
  position_ = new_chunk;
  end_ = new_chunk + alloc_size;
}

inline byte *ChunkedVector::append() noexcept {
  if (position_ == end_) {
    AllocateChunk();
  }

  byte *const result = position_;
  position_ += element_size_;
  num_elements_++;
  return result;
}

}  // namespace tpl::util