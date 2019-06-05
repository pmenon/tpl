#pragma once

#include "sql/memory_pool.h"
#include "util/common.h"
#include "util/macros.h"
#include "util/region.h"

namespace tpl::sql {

/**
 * Stores information for one execution of a plan.
 */
class ExecutionContext {
 public:
  /**
   * An allocator for short-ish strings. Needed because the requirements of
   * string allocation (small size and frequent usage) are slightly different
   * than that of generic memory allocator for larger structures. This string
   * allocator relies on memory regions for fast allocations, and bulk
   * deletions.
   */
  class StringAllocator {
   public:
    /**
     * Create a new allocator
     */
    StringAllocator();

    /**
     * This class cannot be copied or moved.
     */
    DISALLOW_COPY_AND_MOVE(StringAllocator);

    /**
     * Destroy allocator
     */
    ~StringAllocator();

    /**
     * Allocate a string of the given size..
     * @param size Size of the string in bytes.
     * @return A pointer to the string.
     */
    char *Allocate(std::size_t size);

    /**
     * Deallocate a string allocated from this allocator.
     * @param str The string to deallocate.
     */
    void Deallocate(char *str);

   private:
    util::Region region_;
  };

  /**
   * Constructor.
   */
  explicit ExecutionContext(MemoryPool *mem_pool) : mem_pool_(mem_pool) {}

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ExecutionContext);

  /**
   * Return the memory pool.
   */
  MemoryPool *memory_pool() { return mem_pool_; }

  /**
   * Return the string allocator.
   */
  StringAllocator *string_allocator() { return &string_allocator_; }

 private:
  // Pool for memory allocations required during execution
  MemoryPool *mem_pool_;
  // String allocator
  StringAllocator string_allocator_;
};

}  // namespace tpl::sql
