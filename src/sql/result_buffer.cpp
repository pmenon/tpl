#include "sql/result_buffer.h"

#include "sql/planner/plannodes/output_schema.h"
#include "sql/schema.h"
#include "sql/sql.h"
#include "sql/value.h"

namespace tpl::sql {

namespace {

std::size_t ComputeRowSize(const planner::OutputSchema &schema) {
  std::size_t tuple_size = 0;
  for (const auto &col : schema.GetColumns()) {
    switch (GetSqlTypeFromInternalType(col.GetType())) {
      case SqlTypeId::Boolean:
        tuple_size += sizeof(BoolVal);
        break;
      case SqlTypeId::TinyInt:
      case SqlTypeId::SmallInt:
      case SqlTypeId::Integer:
      case SqlTypeId::BigInt:
        tuple_size += sizeof(Integer);
        break;
      case SqlTypeId::Real:
      case SqlTypeId::Double:
      case SqlTypeId::Decimal:
        tuple_size += sizeof(Real);
        break;
      case SqlTypeId::Date:
        tuple_size += sizeof(DateVal);
        break;
      case SqlTypeId::Char:
      case SqlTypeId::Varchar:
        tuple_size += sizeof(StringVal);
        break;
    }
  }
  return tuple_size;
}

}  // namespace

ResultBuffer::ResultBuffer(sql::MemoryPool *memory_pool, const planner::OutputSchema &output_schema,
                           ResultConsumer *consumer, const uint32_t batch_size)
    : tuples_(ComputeRowSize(output_schema), memory_pool),
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
