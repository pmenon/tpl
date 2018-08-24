#pragma once

#include "llvm/Support/Allocator.h"

#include "util/region.h"

namespace tpl::util {

/**
 * An STL-compliant allocator that uses a region-based strategy
 *
 * @tparam T The types of elements this allocator handles
 */
template <typename T>
class StlRegionAllocator {
 public:
  using value_type = T;

  explicit StlRegionAllocator(Region &region) noexcept : region_(region) {}

  StlRegionAllocator(const StlRegionAllocator &other) noexcept
      : region_(other.region_) {}

  template <typename U>
  explicit StlRegionAllocator(const StlRegionAllocator<U> &other) noexcept
      : region_(other.region_) {}

  template <typename U>
  friend class StlRegionAllocator;

  T *allocate(size_t n) { return region_.AllocateArray<T>(n); }

  void deallocate(T *ptr, size_t n) {
    region_.Deallocate(reinterpret_cast<const void *>(ptr), n);
  }

  bool operator==(const StlRegionAllocator &other) const {
    return &region_ == &other.region_;
  }

  bool operator!=(const StlRegionAllocator &other) const {
    return !(this == other);
  }

  Region &region() { return region_; }

 private:
  Region &region_;
};

/**
 * An allocator that complies with LLVM's allocator concept and uses a region-
 * based allocation strategy.
 */
class LlvmRegionAllocator : public llvm::AllocatorBase<LlvmRegionAllocator> {
 public:
  explicit LlvmRegionAllocator(Region &region) noexcept : region_(region) {}

  void *Allocate(size_t size, size_t alignment) {
    return region_.Allocate(size, alignment);
  }

  // Pull in base class overloads.
  using AllocatorBase<LlvmRegionAllocator>::Allocate;

  void Deallocate(const void *ptr, size_t size) {
    region_.Deallocate(ptr, size);
  }

  // Pull in base class overloads.
  using AllocatorBase<LlvmRegionAllocator>::Deallocate;

 private:
  Region &region_;
};

}  // namespace tpl::util