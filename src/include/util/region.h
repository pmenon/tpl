#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <vector>

#include "util/macros.h"

namespace tpl {

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
  void *Allocate(size_t size);

  /**
   * Allocate a (contiguous) array of elements of the given type
   *
   * @tparam T The type of each element in the array
   * @param num_elems The number of requested elements in the array
   * @return
   */
  template <typename T>
  T *AllocateArray(size_t num_elems) {
    return static_cast<T *>(Allocate(num_elems * sizeof(T)));
  }

  /**
   * Free all allocated objects in one (quick) fell swoop
   */
  void FreeAll();

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Simple accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  const std::string &name() const { return name_; }
  uint64_t allocated() const { return allocated_; }
  uint64_t allocated_chunk_bytes() const { return chunk_bytes_allocated_; }

  std::string get_info() const {
    return "Region('" + name() + "',allocated=" + std::to_string(allocated()) +
           ",total chunks=" + std::to_string(allocated_chunk_bytes()) + ")";
  }

 private:
  // Expand the region
  uintptr_t Expand(size_t requested);

  // Round up the given requested size to one that retains byte-alignment
  size_t SizeWithAlignment(size_t size) const {
    return ((size + kByteAlignment - 1) & (-kByteAlignment));
  }

 private:
  /*
   * A chunk represents a contiguous "chunk" of memory. It is the smallest unit
   * of allocation a region acquires from the operating system. Each region
   * allocation is sourced from a chunk.
   */
  struct Chunk {
    Chunk *next;
    uint64_t size;

    void Init(Chunk *next, uint64_t size) {
      this->next = next;
      this->size = size;
    }

    uintptr_t start() { return address(sizeof(Chunk)); }
    uintptr_t end() const { return address(size); }

    uintptr_t address(uint64_t off) const {
      return reinterpret_cast<uintptr_t>(this) + off;
    }
  };

 private:
  // The alignment of all pointers
  static const uint32_t kByteAlignment = 8;

  // Min chunk allocation is 8KB
  static const size_t kMinChunkAllocation = 8 * 1024;

  // Max chunk allocation is 1MB
  static const size_t kMaxChunkAllocation = 1 * 1024 * 1024;

  // The name of the region
  const std::string name_;

  // The number of bytes allocated by this region
  size_t allocated_;

  // The total number of chunk bytes. This may include bytes not yet given out
  // by the region
  size_t chunk_bytes_allocated_;

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
  void *operator new(size_t size) = delete;
  void operator delete(void *ptr) = delete;

  void *operator new(size_t size, Region &region) {
    return region.Allocate(size);
  }

  /*
   * Objects from a Region shouldn't be deleted individually. They'll be deleted
   * when the region is destroyed. You can invoke this behavior manually by
   * calling Region::FreeAll().
   */
  void operator delete(UNUSED void *ptr, UNUSED Region &region) {
    UNREACHABLE();
  };
};

template <typename T>
class RegionPtr {
 public:
  /// Regular constructors

  constexpr RegionPtr() noexcept : ptr_(nullptr) {}
  explicit RegionPtr(T *ptr) noexcept : ptr_(ptr) {}

  /// Move constructors

  RegionPtr(const RegionPtr &other) = delete;
  RegionPtr(RegionPtr &&other) noexcept : ptr_(other.release()) {}

  template <typename U,
            typename = std::enable_if_t<std::is_convertible_v<U, T>>>
  RegionPtr(RegionPtr<U> &&other) noexcept : ptr_(other.release()) {}

  /// Assignment

  RegionPtr &operator=(const RegionPtr &&other) = delete;
  RegionPtr &operator=(RegionPtr &&other) noexcept {
    ptr_ = other.release();
    return *this;
  }

  template <typename U,
            typename = std::enable_if_t<std::is_convertible_v<U, T>>>
  RegionPtr &operator=(RegionPtr<U> &&other) noexcept {
    ptr_ = other.release();
    return *this;
  }

  /// Public API

  T *get() const noexcept { return ptr_; }

  T *release() {
    auto *tmp = ptr_;
    ptr_ = nullptr;
    return tmp;
  }

  T *operator->() const noexcept { return get(); }

  T &operator*() const { return *ptr_; }

  explicit operator bool() const noexcept {
    return get() != nullptr;
  }

 private:
  T *ptr_;
};

}  // namespace tpl