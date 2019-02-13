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
class RegionAllocator {
 public:
  using value_type = T;

  explicit RegionAllocator(Region *region) noexcept : region_(region) {}

  RegionAllocator(const RegionAllocator &other) noexcept
      : region_(other.region_) {}

  template <typename U>
  explicit RegionAllocator(const RegionAllocator<U> &other) noexcept
      : region_(other.region_) {}

  template <typename U>
  friend class RegionAllocator;

  T *allocate(std::size_t n) { return region_->AllocateArray<T>(n); }

  // No-op
  void deallocate(T *ptr, std::size_t n) {}

  bool operator==(const RegionAllocator &other) const {
    return region_ == other.region_;
  }

  bool operator!=(const RegionAllocator &other) const {
    return !(*this == other);
  }

 private:
  Region *region_;
};

/**
 * An allocator that complies with LLVM's allocator concept and uses a region-
 * based allocation strategy.
 */
class LlvmRegionAllocator : public llvm::AllocatorBase<LlvmRegionAllocator> {
 public:
  explicit LlvmRegionAllocator(Region *region) noexcept : region_(region) {}

  void *Allocate(std::size_t size, std::size_t alignment) {
    return region_->Allocate(size, alignment);
  }

  // Pull in base class overloads.
  using AllocatorBase<LlvmRegionAllocator>::Allocate;

  void Deallocate(const void *ptr, std::size_t size) {
    region_->Deallocate(ptr, size);
  }

  // Pull in base class overloads.
  using AllocatorBase<LlvmRegionAllocator>::Deallocate;

 private:
  Region *region_;
};

}  // namespace tpl::util
