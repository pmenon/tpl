#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "common/macros.h"
#include "util/math_util.h"

namespace tpl::util {

/**
 * A region-based allocator supports fast O(1) time allocations of small chunks of memory.
 * Individual de-allocations are not supported, but the entire region can be de-allocated in one
 * fast operation upon destruction. Regions are used to hold ephemeral objects that are allocated
 * once and freed all at once. This is the pattern used during parsing when generating AST nodes
 * which are thrown away after compilation to bytecode.
 */
class Region {
 public:
  /**
   * Construct a region with the given name @em name. No allocations are performed upon
   * construction, only at the first call to @em Allocate().
   */
  explicit Region(std::string_view name) noexcept;

  /**
   * Default move constructor.
   */
  Region(Region &&that) noexcept
      : name_(that.name_),
        allocated_(that.allocated_),
        alignment_waste_(that.alignment_waste_),
        chunk_bytes_allocated_(that.chunk_bytes_allocated_),
        head_(that.head_),
        position_(that.position_),
        end_(that.end_) {
    that.name_ = nullptr;
    that.allocated_ = 0;
    that.alignment_waste_ = 0;
    that.chunk_bytes_allocated_ = 0;
    that.head_ = nullptr;
    that.position_ = 0;
    that.end_ = 0;
  }

  /**
   * Regions cannot be copied.
   */
  DISALLOW_COPY(Region);

  /**
   * Destructor. All allocated memory is freed here.
   */
  ~Region();

  /**
   * Default move assignment.
   */
  Region &operator=(Region &&that) noexcept {
    std::swap(name_, that.name_);
    std::swap(allocated_, that.allocated_);
    std::swap(alignment_waste_, that.alignment_waste_);
    std::swap(chunk_bytes_allocated_, that.chunk_bytes_allocated_);
    std::swap(head_, that.head_);
    std::swap(position_, that.position_);
    std::swap(end_, that.end_);
    return *this;
  }

  /**
   * Allocate memory from this region
   * @param size The number of bytes to allocate
   * @param alignment The desired alignment
   * @return A pointer to the start of the allocated space
   */
  void *Allocate(std::size_t size, std::size_t alignment = kDefaultByteAlignment);

  /**
   * Individual de-allocations in a region-allocator are a no-op. All memory is freed when the
   * region is destroyed, or manually through a call to Region::FreeAll().
   * @param ptr The pointer to the memory we're de-allocating
   * @param size The number of bytes the pointer points to
   */
  void Deallocate(const void *ptr, std::size_t size) const {
    // No-op
  }

  /**
   * Free all allocated objects in one fell swoop
   */
  void FreeAll();

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  // The name of the region
  const char *name() const { return name_; }

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
  // A chunk represents a physically contiguous "chunk" of memory. It is the
  // smallest unit of allocation a region acquires from the operating system.
  // Each individual region allocation is sourced from a chunk.
  struct Chunk {
    Chunk *next;
    uint64_t size;

    void Init(Chunk *_next, uint64_t _size) {
      next = _next;
      size = _size;
    }

    uintptr_t Start() const { return reinterpret_cast<uintptr_t>(this) + sizeof(Chunk); }

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
  const char *name_;

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

  void *operator new(std::size_t size, Region *region) { return region->Allocate(size); }

  // Objects from a Region shouldn't be deleted individually. They'll be deleted
  // when the region is destroyed. You can invoke this behavior manually by
  // calling Region::FreeAll().
  void operator delete(UNUSED void *ptr, UNUSED Region *region) {
    UNREACHABLE("Calling \"delete\" on region object is forbidden!");
  }
};

}  // namespace tpl::util
