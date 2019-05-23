#pragma once

#include <atomic>
#include <memory>

#include "util/common.h"
#include "util/macros.h"
#include "util/spin_latch.h"

namespace tpl::sql {

class MemoryTracker;

/**
 * A memory pool
 */
class MemoryPool {
 public:
  /**
   * Create a pool that reports to the given memory tracker @em tracker.
   * @param tracker The tracker to use to report allocations.
   */
  explicit MemoryPool(MemoryTracker *tracker);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(MemoryPool);

  /**
   * Allocate @em size bytes of memory from this pool.
   * @param size The number of bytes to allocate.
   * @param clear Whether to clear the bytes before returning.
   * @return A pointer to at least @em size bytes of memory.
   */
  void *Allocate(std::size_t size, bool clear);

  /**
   * Allocate @em size bytes of memory from this pool with a specific alignment.
   * @param size The number of bytes to allocate.
   * @param alignment The desired alignment of the allocation.
   * @param clear Whether to clear the bytes before returning.
   * @return A pointer to at least @em size bytes of memory.
   */
  void *AllocateAligned(std::size_t size, std::size_t alignment, bool clear);

  /**
   * Allocate a contiguous array of elements of the given type from this pool.
   * The alignment of the array will be the alignment of the type @em T.
   * @tparam T The type of each element in the array.
   * @param num_elems The number of requested elements in the array.
   * @return An array pointer to at least @em num_elems elements of type @em T
   */
  template <typename T>
  T *AllocateArray(std::size_t num_elems, bool clear) {
    return reinterpret_cast<T *>(
        AllocateAligned(sizeof(T) * num_elems, alignof(T), clear));
  }

  /**
   * Deallocate memory allocated by this pool.
   * @param ptr The pointer to the memory.
   * @param size The size of the allocation.
   */
  void Deallocate(void *ptr, std::size_t size);

  /**
   * Set the global threshold for when to use MMap and huge pages versus
   * standard allocations.
   * @param size The size threshold.
   */
  static void SetMMapSizeThreshold(std::size_t size);

 private:
  // Metadata tracker for memory allocations
  MemoryTracker *tracker_;

  //
  static std::atomic<u64> kMmapThreshold;
};

/**
 * An STL-compliant allocator that uses an injected memory pool
 * @tparam T The types of elements this allocator handles
 */
template <typename T>
class MemoryPoolAllocator {
 public:
  using value_type = T;

  MemoryPoolAllocator(MemoryPool *memory) : memory_(memory) {}  // NOLINT

  MemoryPoolAllocator(const MemoryPoolAllocator &other)
      : memory_(other.memory_) {}

  template <typename U>
  MemoryPoolAllocator(const MemoryPoolAllocator<U> &other)  // NOLINT
      : memory_(other.memory_) {}

  template <typename U>
  friend class MemoryPoolAllocator;

  T *allocate(std::size_t n) { return memory_->AllocateArray<T>(n, false); }

  void deallocate(T *ptr, std::size_t n) { memory_->Deallocate(ptr, n); }

  bool operator==(const MemoryPoolAllocator &other) const {
    return memory_ == other.memory_;
  }

  bool operator!=(const MemoryPoolAllocator &other) const {
    return !(*this == other);
  }

 private:
  MemoryPool *memory_;
};

}  // namespace tpl::sql
