#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>

#include "util/macros.h"
#include "util/math_util.h"

namespace tpl::util {

/**
 * A region-based allocator supports fast allocations of small chunks of memory.
 * Individual deallocations aren't supported, but the entire region can be
 * deallocated in one fast operation. Regions are used to hold ephemeral objects
 * that are allocated once and freed all at once. This is the pattern used
 * during parsing when generating AST nodes which are thrown away after
 * compilation to bytecode.
 */
class Region {
 public:
  explicit Region(std::string name);

  DISALLOW_COPY_AND_MOVE(Region);

  ~Region();

  /**
   * Allocate memory from this region
   *
   * @param size The number of bytes to allocate
   * @return A pointer to the start of the allocated space
   */
  void *Allocate(std::size_t size,
                 std::size_t alignment = kDefaultByteAlignment);

  /**
   * Allocate a (contiguous) array of elements of the given type
   *
   * @tparam T The type of each element in the array
   * @param num_elems The number of requested elements in the array
   * @return
   */
  template <typename T>
  T *AllocateArray(std::size_t num_elems) {
    return static_cast<T *>(Allocate(num_elems * sizeof(T), alignof(T)));
  }

  /**
   * Invidivual deallocations in a region-allocator are a no-op. All memory is
   * freed when the region is destroyed, or manually through a call to FreeAll()
   *
   * @param ptr The pointer to the memory we're deallocating
   * @param size The number of bytes the pointer points to
   */
  void Deallocate(const void *ptr, std::size_t size) const {
    // No-op
  }

  /**
   * Free all allocated objects in one fell swoop
   */
  void FreeAll();

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Simple accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  // The name of the region
  const std::string &name() const { return name_; }

  // The number of bytes this region has given out
  uint64_t allocated() const { return allocated_; }

  // The number of bytes wasted due to alignment requirements
  uint64_t alignment_waste() const { return alignment_waste_; }

  // The total number of bytes acquired from the OS
  uint64_t total_memory() const { return chunk_bytes_allocated_; }

 private:
  // Expand the region
  uintptr_t Expand(std::size_t requested);

 private:
  /*
   * A chunk represents a contiguous "chunk" of memory. It is the smallest unit
   * of allocation a region acquires from the operating system. Each individual
   * region allocation is sourced from a chunk.
   */
  struct Chunk {
    Chunk *next;
    uint64_t size;

    void Init(Chunk *next, uint64_t size) {
      this->next = next;
      this->size = size;
    }

    uintptr_t Start() const {
      return reinterpret_cast<uintptr_t>(this) + sizeof(Chunk);
    }
    uintptr_t End() const { return reinterpret_cast<uintptr_t>(this) + size; }
  };

 private:
  // The alignment of all pointers
  static const uint32_t kDefaultByteAlignment = 8;

  // Min chunk allocation is 8KB
  static const std::size_t kMinChunkAllocation = 8 * 1024;

  // Max chunk allocation is 1MB
  static const std::size_t kMaxChunkAllocation = 1 * 1024 * 1024;

  // The name of the region
  const std::string name_;

  // The number of bytes allocated by this region
  std::size_t allocated_;

  // Bytes wasted due to alignment
  std::size_t alignment_waste_;

  // The total number of chunk bytes. This may include bytes not yet given out
  // by the region
  std::size_t chunk_bytes_allocated_;

  // The head of the chunk list
  Chunk *head_;

  // The position in the current free chunk where the next allocation can happen
  // These two fields make up the contiguous space [position, limit) where
  // allocations can happen from
  uintptr_t position_;
  uintptr_t end_;
};

/**
 * Base class for objects allocated from a region
 */
class RegionObject {
 public:
  // Region objects should always be allocated from and release a region
  void *operator new(std::size_t size) = delete;
  void operator delete(void *ptr) = delete;

  void *operator new(std::size_t size, Region &region) {
    return region.Allocate(size);
  }

  /*
   * Objects from a Region shouldn't be deleted individually. They'll be deleted
   * when the region is destroyed. You can invoke this behavior manually by
   * calling Region::FreeAll().
   */
  void operator delete(UNUSED void *ptr, UNUSED Region &region) {
    UNREACHABLE("Calling \"delete\" on region object is forbidden!");
  };
};

}  // namespace tpl::util