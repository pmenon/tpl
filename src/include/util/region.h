#pragma once

#include <cstdint>
#include <string>
#include <type_traits>

#include "util/macros.h"

namespace tpl {

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
  void *Allocate(std::size_t size);

  const std::string &name() const { return name_; }
  uint64_t allocated() const { return allocated_; }
  uint64_t allocated_chunk_bytes() const { return chunk_bytes_allocated_; }

  std::string get_info() const {
    return "Region(" + name() + ",allocated=" + std::to_string(allocated()) +
           ",total chunks=" + std::to_string(allocated_chunk_bytes()) + ")";
  }

 private:
  // Expand the region
  uintptr_t Expand();

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
  static constexpr uint32_t kAlignment = 8;

  // The name of the region
  const std::string name_;

  // The number of bytes allocated by this region
  std::size_t allocated_;

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

class RegionObject {
 public:
  void *operator new(std::size_t size, Region &region) {
    return region.Allocate(size);
  }

  void operator delete(UNUSED void *ptr) = delete;
  void operator delete(UNUSED void *ptr, UNUSED Region &region) = delete;
};

}  // namespace tpl