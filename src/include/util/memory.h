#pragma once

#include <sys/mman.h>
#include <cstddef>
#include <cstring>
#if defined(__APPLE__)
#include <malloc.h>
#endif

#include "util/common.h"
#include "util/macros.h"

// Needed for some Darwin machine that don't have MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

namespace tpl::util {

// ---------------------------------------------------------
// Allocations
// ---------------------------------------------------------

inline void *MallocAligned(const std::size_t size,
                           const std::size_t alignment) {
  TPL_ASSERT(alignment % sizeof(void *) == 0,
             "Alignment must be a multiple of sizeof(void*)");
  TPL_ASSERT((alignment & (alignment - 1)) == 0,
             "Alignment must be a power of two");

  void *ptr = nullptr;
#if defined(__APPLE__)
  i32 ret = posix_memalign(&ptr, alignment, size);
  TPL_ASSERT(ret == 0, "Allocation failed")
#else
  ptr = std::aligned_alloc(alignment, size);
#endif
  return ptr;
}

inline void *MallocHuge(std::size_t size) {
  // Attempt to map
  void *ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

  // If failed, return null. Let client worry.
  if (ptr == MAP_FAILED) {
    return nullptr;
  }

  // All good, advise to use huge pages
#if !defined(__APPLE__)
  madvise(ptr, size, MADV_HUGEPAGE);
#endif

  // Done
  return ptr;
}

template <typename T>
inline void *MallocHuge() {
  return MallocHuge(sizeof(T));
}

template <typename T>
inline T *MallocHugeArray(std::size_t num_elems) {
  std::size_t size = sizeof(T) * num_elems;
  void *ptr = MallocHuge(size);
  return reinterpret_cast<T *>(ptr);
}

inline void FreeHuge(void *ptr, std::size_t size) { munmap(ptr, size); }

template <typename T>
inline void FreeHugeArray(T *ptr, std::size_t num_elems) {
  FreeHuge(static_cast<void *>(ptr), sizeof(T) * num_elems);
}

// ---------------------------------------------------------
// Prefetch
// ---------------------------------------------------------

template <bool READ, Locality LOCALITY>
inline void Prefetch(const void *const addr) noexcept {
  // The builtin prefetch intrinsic takes three arguments:
  // 'addr': the address we want to prefetch
  // 'rw': indicates read-write intention; 0 is for a read, 1 is for a write
  // 'locality': indicates the degree of temporal locality represented in the
  // range {0-3}. 0 means no locality; 3 is high temporal locality.
  __builtin_prefetch(addr, READ ? 0 : 1, static_cast<u8>(LOCALITY));
}

}  // namespace tpl::util
