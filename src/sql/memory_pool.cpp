#include "sql/memory_pool.h"

#include <cstdlib>
#include <memory>

#include "common/memory.h"
#include "sql/memory_tracker.h"

namespace tpl::sql {

// If the allocation size is larger than this value, use huge pages
std::atomic<std::size_t> MemoryPool::kMmapThreshold = 64 * MB;

// Minimum alignment to abide by
static constexpr uint32_t kMinMallocAlignment = 8;

MemoryPool::MemoryPool(MemoryTracker *tracker) : tracker_(tracker) { (void)tracker_; }

void *MemoryPool::Allocate(const std::size_t size, const bool clear) {
  return AllocateAligned(size, 0, clear);
}

void *MemoryPool::AllocateAligned(const std::size_t size, const std::size_t alignment,
                                  const bool clear) {
  void *buf = nullptr;

  if (size >= kMmapThreshold.load(std::memory_order_relaxed)) {
    buf = Memory::MallocHuge(size, true);
    TPL_ASSERT(buf != nullptr, "Null memory pointer");
    // No need to clear memory, guaranteed on Linux
  } else {
    if (alignment < kMinMallocAlignment) {
      if (clear) {
        buf = std::calloc(size, 1);
      } else {
        buf = std::malloc(size);
      }
    } else {
      buf = Memory::MallocAligned(size, alignment);
      if (clear) {
        std::memset(buf, 0, size);
      }
    }
  }

  // Done
  return buf;
}

void MemoryPool::Deallocate(void *ptr, std::size_t size) {
  if (size >= kMmapThreshold.load(std::memory_order_relaxed)) {
    Memory::FreeHuge(ptr, size);
  } else {
    std::free(ptr);
  }
}

void MemoryPool::SetMMapSizeThreshold(const std::size_t size) { kMmapThreshold = size; }

}  // namespace tpl::sql
