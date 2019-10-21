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
   * Create a context for the execution of a query. All allocations will occur from the provided
   * memory allocator @em mem_pool. If a schema and result consumer are provided, the result of the
   * query will be fed into the @em consumer.
   * @param mem_pool The memory pool for all memory allocations.
   * @param schema The optional schema of the output.
   * @param consumer The optional consumer of the output.
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
   * @return The query context's memory pool.
   */
  MemoryPool *GetMemoryPool() { return mem_pool_; }

  /**
   * @return The allocator used for all temporary and ephemeral strings the query needs.
   */
  VarlenHeap *GetStringHeap() { return &string_allocator_; }

  /**
   * @return The result buffer, if any.
   */
  ResultBuffer *GetResultBuffer() { return buffer_.get(); }

 private:
  // Pool for memory allocations required during execution
  MemoryPool *mem_pool_;

  // String allocator
  VarlenHeap string_allocator_;

  // Buffer of results before sending to consumer
  std::unique_ptr<ResultBuffer> buffer_;
};

}  // namespace tpl::sql
