#pragma once

#include "sql/memory_pool.h"
#include "util/common.h"
#include "util/macros.h"

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

 private:
  // Pool for memory allocations required during execution
  MemoryPool *mem_pool_;
};

}  // namespace tpl::sql
