#pragma once

#include <sys/mman.h>
#include <cstddef>
#include <cstring>

namespace tpl::util::mem {

inline void *MallocHuge(std::size_t size) {
  void *ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
#if !defined(__APPLE__)
  madvise(ptr, size, MADV_HUGEPAGE);
#endif
  std::memset(ptr, 0, size);
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

}  // namespace tpl::util::mem
