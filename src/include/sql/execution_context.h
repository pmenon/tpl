#pragma once

#include <memory>

#include "common/common.h"
#include "common/macros.h"
#include "sql/result_buffer.h"
#include "sql/runtime_types.h"

namespace tpl::sql {

class MemoryPool;
class Schema;

/**
 * Stores information for one execution of a plan.
 */
class ExecutionContext {
 public:
  /**
   * Constructor.
   */
  /**
   * Create a context for the execution of a query. All allocations will occur from the provided
   * memory allocator @em mem_pool. If a schema and result consumer are provided, the result of the
   * query will be fed into the @em consumer.
   * @param mem_pool The memory pool for all memory allocations.
   * @param schema
   * @param consumer
   */
  explicit ExecutionContext(MemoryPool *mem_pool, const Schema *schema = nullptr,
                            ResultConsumer *consumer = nullptr)
      : mem_pool_(mem_pool),
        buffer_(schema == nullptr ? nullptr : new ResultBuffer(mem_pool, *schema, consumer)) {}

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

  /**
   * Return the result buffer, if any.
   */
  ResultBuffer *result_buffer() { return buffer_.get(); }

 private:
  // Pool for memory allocations required during execution
  MemoryPool *mem_pool_;
  // String allocator
  VarlenHeap string_allocator_;
  // Buffer of results before sending to consumer
  std::unique_ptr<ResultBuffer> buffer_;
};

}  // namespace tpl::sql
