#pragma once

#include "llvm/Support/Allocator.h"

#include "util/region.h"

namespace tpl::util {

/// An STL-compliant allocator that uses a region-based strategy
/// \tparam T The types of elements this allocator handles
template <typename T>
class StlRegionAllocator {
 public:
  using value_type = T;

  StlRegionAllocator(Region *region) noexcept : region_(region) {}  // NOLINT

  StlRegionAllocator(const StlRegionAllocator &other) noexcept : region_(other.region_) {}

  template <typename U>
  StlRegionAllocator(const StlRegionAllocator<U> &other) noexcept  // NOLINT
      : region_(other.region_) {}

  template <typename U>
  friend class StlRegionAllocator;

  T *allocate(std::size_t n) { return region_->AllocateArray<T>(n); }

  void deallocate(T *ptr, std::size_t n) { region_->Deallocate(ptr, n); }

  bool operator==(const StlRegionAllocator &other) const { return region_ == other.region_; }

  bool operator!=(const StlRegionAllocator &other) const { return !(*this == other); }

 private:
  Region *region_;
};

/// An allocator that complies with LLVM's allocator concept and uses a region-
/// based allocation strategy.
class LLVMRegionAllocator : public llvm::AllocatorBase<LLVMRegionAllocator> {
 public:
  explicit LLVMRegionAllocator(Region *region) noexcept : region_(region) {}

  void *Allocate(std::size_t size, std::size_t alignment) {
    return region_->Allocate(size, alignment);
  }

  // Pull in base class overloads.
  using AllocatorBase<LLVMRegionAllocator>::Allocate;

  void Deallocate(const void *ptr, std::size_t size) { region_->Deallocate(ptr, size); }

  // Pull in base class overloads.
  using AllocatorBase<LLVMRegionAllocator>::Deallocate;

 private:
  Region *region_;
};

}  // namespace tpl::util
