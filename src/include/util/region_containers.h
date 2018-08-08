#pragma once

#include <vector>

#include "util/region_allocator.h"

namespace tpl::util {

template <typename T>
class RegionVector : public std::vector<T, RegionAllocator<T>> {
 public:
  explicit RegionVector(Region &region)
      : std::vector<T, RegionAllocator<T>>(RegionAllocator<T>(region)) {}

  RegionVector(size_t n, Region &region)
      : std::vector<T, RegionAllocator<T>>(n, RegionAllocator<T>(region)) {}

  RegionVector(size_t n, const T &elem, Region &region)
      : std::vector<T, RegionAllocator<T>>(n, elem,
                                           RegionAllocator<T>(region)) {}

  RegionVector(std::initializer_list<T> list, Region &region)
      : std::vector<T, RegionAllocator<T>>(list, RegionAllocator<T>(region)) {}

  template <typename InputIter>
  RegionVector(InputIter first, InputIter last, Region &region)
      : std::vector<T, RegionAllocator<T>>(first, last,
                                           RegionAllocator<T>(region)) {}
};

}  // namespace tpl::util