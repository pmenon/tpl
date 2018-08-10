#pragma once

#include <unordered_map>
#include <vector>

#include "util/region_allocator.h"

namespace tpl::util {

/**
 * STL vectors backed by a region allocator
 */
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

/**
 * STL hash maps backed by a region allocator
 */
template <typename Key, typename Value, typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
class RegionUnorderedMap
    : public std::unordered_map<Key, Value, Hash, KeyEqual,
                                RegionAllocator<std::pair<const Key, Value>>> {
 public:
  explicit RegionUnorderedMap(Region &region)
      : std::unordered_map<Key, Value, Hash, KeyEqual,
                           RegionAllocator<std::pair<const Key, Value>>>(
            RegionAllocator<std::pair<const Key, Value>>(region)) {}
};

}  // namespace tpl::util