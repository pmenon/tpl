#pragma once

#include "llvm/Support/Allocator.h"

#include "util/region.h"

namespace tpl::util {

/**
 * An STL-compliant allocator that uses a region-based strategy.
 * @tparam T The types of elements this allocator handles.
 */
template <typename T>
class StlRegionAllocator {
 public:
  using value_type = T;

  /**
   * Instantiate an allocator using the provided region.
   * @param region The region to allocate from.
   */
  StlRegionAllocator(Region *region) noexcept : region_(region) {}  // NOLINT

  /**
   * Instantiate an allocator using the region from a different allocator.
   * @param other The other allocator.
   */
  StlRegionAllocator(const StlRegionAllocator &other) noexcept : region_(other.region_) {}

  /**
   * Adapt an allocator for a different type. Uses the underlying region.
   * @tparam U The type of elements allocated by the other allocator.
   * @param other The other allocator.
   */
  template <typename U>
  StlRegionAllocator(const StlRegionAllocator<U> &other) noexcept  // NOLINT
      : region_(other.region_) {}

  template <typename U>
  friend class StlRegionAllocator;

  /**
   * Allocate memory of the given size.
   * @param n The size, in bytes, to allocate.
   * @return A pointer to the allocated memory.
   */
  T *allocate(std::size_t n) {
    return static_cast<T *>(region_->Allocate(sizeof(T) * n, alignof(T)));
  }

  /**
   * Deallocate memory of the given size pointed to by the provided pointer.
   * @param ptr The pointer to the memory region to de-allocate.
   * @param n The size, in bytes, of the memory region to de-allocate.
   */
  void deallocate(T *ptr, std::size_t n) { region_->Deallocate(ptr, n); }

  /**
   * @return True if this and that allocators allocate the same object types, and use the same
   *         underlying region. False otherwise.
   */
  bool operator==(const StlRegionAllocator &other) const { return region_ == other.region_; }

  bool operator!=(const StlRegionAllocator &other) const { return !(*this == other); }

 private:
  Region *region_;
};

/**
 * A region allocator that complies with LLVM's allocator concept.
 */
class LLVMRegionAllocator : public llvm::AllocatorBase<LLVMRegionAllocator> {
 public:
  /**
   * Instantiate an allocator using the provided region.
   * @param region The region to allocate from.
   */
  explicit LLVMRegionAllocator(Region *region) noexcept : region_(region) {}

  /**
   * Allocate memory of the given size and alignment.
   * @param size The number of bytes to allocate.
   * @param alignment The desired alignment.
   * @return A pointer to the aligned memory of the given size.
   */
  void *Allocate(size_t size, size_t alignment) { return region_->Allocate(size, alignment); }

  /** Pull in base class overloads. */
  using AllocatorBase<LLVMRegionAllocator>::Allocate;

  /**
   * De-allocate memory pointed to by the provided pointer and of the given size and alignment.
   * @param ptr Pointer to the memory to de-allocate.
   * @param size The size (in bytes) of the memory region to de-allocate.
   * @param alignment The alignment of the memory region to de-allocate.
   */
  void Deallocate(const void *ptr, size_t size, UNUSED size_t alignment) {
    region_->Deallocate(ptr, size);
  }

  /** Pull in base class overloads. */
  using AllocatorBase<LLVMRegionAllocator>::Deallocate;

 private:
  Region *region_;
};

}  // namespace tpl::util
