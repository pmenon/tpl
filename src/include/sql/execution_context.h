#pragma once

#include "common/common.h"
#include "common/macros.h"
#include "sql/memory_pool.h"
#include "sql/runtime_types.h"

namespace tpl::sql {

/**
 * Stores information for one execution of a plan.
 */
class ExecutionContext {
 public:
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
  VarlenHeap *string_allocator() { return &string_allocator_; }

 private:
  // Pool for memory allocations required during execution
  MemoryPool *mem_pool_;
  // String allocator
  VarlenHeap string_allocator_;
};

}  // namespace tpl::sql
