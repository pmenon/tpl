#include "sql/result_buffer.h"

#include "sql/planner/plannodes/output_schema.h"
#include "sql/schema.h"
#include "sql/sql.h"
#include "sql/value.h"

namespace tpl::sql {

ResultBuffer::ResultBuffer(sql::MemoryPool *memory_pool, const planner::OutputSchema &output_schema,
                           ResultConsumer *consumer, const uint32_t batch_size)
    : tuples_(output_schema.ComputeOutputRowSize(), memory_pool),
      consumer_(consumer),
      batch_size_(batch_size) {}

ResultBuffer::~ResultBuffer() = default;

void ResultBuffer::Finalize() {
  util::SpinLatch::ScopedSpinLatch latch(&output_latch_);
  if (!tuples_.empty()) {
    consumer_->Consume(tuples_);
  }
}

}  // namespace tpl::sql
