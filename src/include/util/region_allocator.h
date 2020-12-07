#pragma once

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
   * @return True if the storage allocated by @em this allocator can be deallocated through @em that
   *         allocator. Establishes reflexive, symmetric, and transitive relationship. Does not
   *         throw exceptions.
   */
  bool operator==(const StlRegionAllocator &that) const noexcept {
    return this == &that || region_ == that.region_;
  }

  /**
   * @return True if the storage allocated by @em this allocator cannot be deallocated through
   *         @em that.
   */
  bool operator!=(const StlRegionAllocator &that) const noexcept { return !(*this == that); }

 private:
  // The region to source memory from.
  Region *region_;
};

}  // namespace tpl::util
