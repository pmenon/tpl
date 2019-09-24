#pragma once

#include "sql/memory_pool.h"
#include "util/chunked_vector.h"

namespace tpl::sql {

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

class NoOpResultConsumer : public ResultConsumer {
 public:
  ~NoOpResultConsumer() override {}

  void Consume(UNUSED const OutputBuffer &batch) override {}
};

}  // namespace tpl::sql
