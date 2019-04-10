#include "sql/table_vector_iterator.h"
#include "sql/execution_structures.h"

namespace tpl::sql {

using namespace terrier;

TableVectorIterator::TableVectorIterator(const storage::SqlTable &table,
                                         const catalog::Schema &schema,
                                         transaction::TransactionContext *txn)
    : table_(table),
      schema_(schema),
      txn_(txn),
      initializer_(GetInitializer(table, schema)),
      iter_(table.begin()),
      first_call_(true) {
  std::vector<catalog::col_oid_t> col_oids;
  const std::vector<catalog::Schema::Column> &columns = schema.GetColumns();
  for (const auto &col : columns) {
    col_oids.emplace_back(col.GetOid());
  }
  buffer_ = common::AllocationUtil::AllocateAligned(
      initializer_.first.ProjectedColumnsSize());
  projected_columns_ = initializer_.first.Initialize(buffer_);
}

std::pair<storage::ProjectedColumnsInitializer, storage::ProjectionMap>
TableVectorIterator::GetInitializer(const storage::SqlTable &table,
                                    const catalog::Schema &schema) {
  std::vector<catalog::col_oid_t> col_oids;
  const std::vector<catalog::Schema::Column> &columns = schema.GetColumns();
  for (const auto &col : columns) {
    col_oids.emplace_back(col.GetOid());
  }
  return table.InitializerForProjectedColumns(col_oids, kDefaultVectorSize);
}

size_t i = 0;
bool TableVectorIterator::Advance() {
  if (iter_ == table_.end()) {
    if (txn_ != nullptr) {
      auto *exec = ExecutionStructures::Instance();
      exec->GetTxnManager()->Commit(txn_, [](void *) { return; }, nullptr);
      txn_ = nullptr;
    }
    return false;
  }
  // TODO(Amadou): This is a temporary fix until transactions are part of the
  // language.
  if (txn_ == nullptr) {
    auto *exec = ExecutionStructures::Instance();
    txn_ = exec->GetTxnManager()->BeginTransaction();
  }
  table_.Scan(txn_, &iter_, projected_columns_);
  vector_projection_iterator_.SetProjectedColumn(projected_columns_);
  return true;
}

}  // namespace tpl::sql
