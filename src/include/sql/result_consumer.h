#pragma once

#include "sql/memory_pool.h"
#include "util/chunked_vector.h"

namespace tpl::sql {

/**
 * Data structure representing a buffer of tuples.
 */
using OutputBuffer = util::ChunkedVector<MemoryPoolAllocator<byte>>;

/**
 * Interface class to consume the results of a query.
 */
class ResultConsumer {
 public:
  /**
   * Destructor.
   */
  virtual ~ResultConsumer() = default;

  /**
   * Consume a batch of results.
   * @param batch The batch of tuples.
   */
  virtual void Consume(const OutputBuffer &batch) = 0;
};

/**
 * A consumer that doesn't do anything with the result tuples.
 */
class NoOpResultConsumer : public ResultConsumer {
 public:
  /**
   * Simple destructor.
   */
  ~NoOpResultConsumer() override = default;

  /**
   * No-op batch consumption.
   * @param batch The output batch to consume.
   */
  void Consume(UNUSED const OutputBuffer &batch) override {}
};

}  // namespace tpl::sql
