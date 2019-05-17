#pragma once

#include <memory>
#include <vector>

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
   * Constructor.
   */
  ExecutionContext(util::Region *mem_pool, u32 query_state_size)
      : mem_pool_(mem_pool),
        query_state_(mem_pool_->AllocateArray<byte>(query_state_size)) {}

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ExecutionContext);

  /**
   * Return the memory pool.
   */
  util::Region *memory_pool() { return mem_pool_; }

  /**
   * Access the state for the query.
   */
  byte *query_state() { return query_state_; }

 private:
  // Temporary memory pool for allocations done during execution
  util::Region *mem_pool_;
  // The query state. Opaque because it's defined in generated code.
  byte *query_state_;
};

}  // namespace tpl::sql
