#pragma once

#include "util/region.h"

namespace tpl::util {

/**
 * An allocator that uses a region-based strategy
 *
 * @tparam T The types of elements this allocator handles
 */
template <typename T>
class RegionAllocator {
 public:
  using value_type = T;

  explicit RegionAllocator(Region &region) : region_(region) {}
  RegionAllocator(const RegionAllocator &other)
      : region_(other.region_) {}
  template <typename U>
  explicit RegionAllocator(const RegionAllocator<U> &other)
      : region_(other.region_) {}

  T *allocate(size_t n) { return region_.AllocateArray<T>(n); }

  void deallocate(UNUSED T *ptr, UNUSED size_t n) {}

  bool operator==(const RegionAllocator &other) const {
    return &region_ == &other.region_;
  }

  bool operator!=(const RegionAllocator &other) const {
    return !(this == other);
  }

  Region &region() { return region_; }

 private:
  Region &region_;
};

}  // namespace tpl::util