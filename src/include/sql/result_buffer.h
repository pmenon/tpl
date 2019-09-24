#pragma once

#include "common/common.h"
#include "sql/result_consumer.h"
#include "util/chunked_vector.h"

namespace tpl::sql {

class MemoryPool;
class Schema;

/**
 * A class that buffers the output and makes a callback for every batch.
 */
class ResultBuffer {
 public:
  /**
   * Batch size
   */
  static constexpr uint32_t BATCH_SIZE = 32;

  /**
   * Construct a buffer.
   * @param memory_pool memory pool to use for buffer allocation
   * @param output_schema The schema of the output
   * @param callback upper layer callback
   */
  ResultBuffer(sql::MemoryPool *memory_pool, const sql::Schema &output_schema,
               ResultConsumer *consumer);

  /**
   * Destructor
   */
  ~ResultBuffer();

  /**
   * @return an output slot to be written to.
   */
  byte *AllocOutputSlot() {
    if (tuples_.size() == BATCH_SIZE) {
      consumer_->Consume(tuples_);
      tuples_.clear();
    }
    return tuples_.append();
  }

  /**
   * Called at the end of execution to return the final few tuples.
   */
  void Finalize();

 private:
  OutputBuffer tuples_;
  ResultConsumer *consumer_;
};

}  // namespace tpl::sql
