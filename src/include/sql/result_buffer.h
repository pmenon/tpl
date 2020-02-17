#pragma once

#include "common/common.h"
#include "sql/result_consumer.h"
#include "util/chunked_vector.h"

namespace tpl::sql {

namespace planner {
class OutputSchema;
}

class MemoryPool;
class Schema;

/**
 * A class that buffers the result tuples from a query into batches and delivers them to the
 * consumer of the query.
 */
class ResultBuffer {
 public:
  /**
   * Default batch size.
   */
  static constexpr uint32_t kDefaultBatchSize = 32;

  /**
   * Construct a buffer.
   * @param memory_pool memory pool to use for buffer allocation.
   * @param output_schema The schema of the output.
   * @param consumer The callback invoked to consume batches of results.
   * @param batch_size The size of the buffer to use to batch results.
   */
  ResultBuffer(sql::MemoryPool *memory_pool, const planner::OutputSchema &output_schema,
               ResultConsumer *consumer, uint32_t batch_size = kDefaultBatchSize);

  /**
   * Destructor.
   */
  ~ResultBuffer();

  /**
   * @return an output slot to be written to.
   */
  byte *AllocOutputSlot() {
    util::SpinLatch::ScopedSpinLatch latch(&output_latch_);
    if (tuples_.size() == GetBatchSize()) {
      consumer_->Consume(tuples_);
      tuples_.clear();
    }
    return tuples_.append();
  }

  /**
   * Called at the end of execution to return the final few tuples.
   */
  void Finalize();

  /**
   * @return The maximum size of output batches sent to the consumer.
   */
  uint32_t GetBatchSize() const noexcept { return batch_size_; }

 private:
  // Buffer storing output tuples
  OutputBuffer tuples_;

  // The consumer of the results
  ResultConsumer *consumer_;

  // The batch size
  uint32_t batch_size_;

  // Lock for parallel output.
  // TODO(Amadou): Should parallel output even be allowed?
  util::SpinLatch output_latch_;
};

}  // namespace tpl::sql
