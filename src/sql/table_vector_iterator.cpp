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
      null_txn_(txn == nullptr) {
  // Get the column oids for the projected columns
  std::vector<catalog::col_oid_t> col_oids;
  const std::vector<catalog::Schema::Column> &columns = schema.GetColumns();
  for (const auto &col : columns) {
    col_oids.emplace_back(col.GetOid());
  }
  // Allocated a buffer.
  buffer_ = common::AllocationUtil::AllocateAligned(
      initializer_.first.ProjectedColumnsSize());
  // Initilize the projected columns
  projected_columns_ = initializer_.first.Initialize(buffer_);
}

// Helper method to get the PC initialiazer.
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

bool TableVectorIterator::Advance() {
  // First check if the iterator ended.
  if (iter_ == table_.end()) {
    // If so, try to commit if we were the ones who created this transaction.
    if (null_txn_ && txn_ != nullptr) {
      auto *exec = ExecutionStructures::Instance();
      exec->GetTxnManager()->Commit(txn_, [](void *) { return; }, nullptr);
      txn_ = nullptr;
    }
    // End the iteration.
    return false;
  }
  // TODO(Amadou): This is a temporary fix until transactions are part of the
  // language.
  // Begin a new transaction if none was passed in.
  if (txn_ == nullptr) {
    auto *exec = ExecutionStructures::Instance();
    txn_ = exec->GetTxnManager()->BeginTransaction();
  }
  // Scan the table a set the projected column.
  table_.Scan(txn_, &iter_, projected_columns_);
  vector_projection_iterator_.SetProjectedColumn(projected_columns_);
  return true;
}

}  // namespace tpl::sql
