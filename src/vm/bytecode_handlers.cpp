#include "vm/bytecode_handlers.h"

#include "catalog/catalog_defs.h"
#include "sql/execution_structures.h"

extern "C" {

// ---------------------------------------------------------
// Region
// ---------------------------------------------------------

void OpRegionInit(tpl::util::Region *region) {
  new (region) tpl::util::Region("tmp");
}

void OpRegionFree(tpl::util::Region *region) { region->~Region(); }

// ---------------------------------------------------------
// Transactions
// ---------------------------------------------------------
void OpBeginTransaction(terrier::transaction::TransactionContext **txn) {
  auto *exec = tpl::sql::ExecutionStructures::Instance();
  *txn = exec->GetTxnManager()->BeginTransaction();
}

void OpCommitTransaction(terrier::transaction::TransactionContext **txn) {
  auto *exec = tpl::sql::ExecutionStructures::Instance();
  exec->GetTxnManager()->Commit(*txn, [](void *) { return; }, nullptr);
}

void OpAbortTransaction(terrier::transaction::TransactionContext **txn) {
  auto *exec = tpl::sql::ExecutionStructures::Instance();
  exec->GetTxnManager()->Abort(*txn);
}

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorInit(tpl::sql::TableVectorIterator *iter,
                               u16 table_id) {
  TPL_ASSERT(iter != nullptr, "Null iterator to initialize");

  auto *exec = tpl::sql::ExecutionStructures::Instance();
  auto *table = exec->GetCatalog()->LookupTableById(
      terrier::catalog::table_oid_t(table_id));

  // At this point, the table better exist ...
  TPL_ASSERT(table != nullptr, "Table can't be null!");

  new (iter) tpl::sql::TableVectorIterator(*table->GetTable(),
                                           *table->GetStorageSchema());
}

void OpTableVectorIteratorClose(tpl::sql::TableVectorIterator *iter) {
  TPL_ASSERT(iter != nullptr, "NULL iterator given to close");
  iter->~TableVectorIterator();
}

// ---------------------------------------------------------
// VPI Vectorized Filters
// ---------------------------------------------------------

void OpVPIFilterEqual(u32 *size,
                      UNUSED tpl::sql::VectorProjectionIterator *iter,
                      UNUSED u16 col_id, UNUSED i64 val) {
  *size = 0;
}

void OpVPIFilterGreaterThan(u32 *size,
                            UNUSED tpl::sql::VectorProjectionIterator *iter,
                            UNUSED u16 col_id, UNUSED i64 val) {
  *size = 0;
}

void OpVPIFilterGreaterThanEqual(
    u32 *size, UNUSED tpl::sql::VectorProjectionIterator *iter,
    UNUSED u16 col_id, UNUSED i64 val) {
  *size = 0;
}

void OpVPIFilterLessThan(u32 *size,
                         UNUSED tpl::sql::VectorProjectionIterator *iter,
                         UNUSED u16 col_id, UNUSED i64 val) {
  *size = 0;
}

void OpVPIFilterLessThanEqual(u32 *size,
                              UNUSED tpl::sql::VectorProjectionIterator *iter,
                              UNUSED u16 col_id, UNUSED i64 val) {
  *size = 0;
}

void OpVPIFilterNotEqual(u32 *size,
                         UNUSED tpl::sql::VectorProjectionIterator *iter,
                         UNUSED u16 col_id, UNUSED i64 val) {
  *size = 0;
}

// ---------------------------------------------------------
// Join Hash Table
// ---------------------------------------------------------

void OpJoinHashTableInit(tpl::sql::JoinHashTable *join_hash_table,
                         tpl::util::Region *region, u32 tuple_size) {
  new (join_hash_table) tpl::sql::JoinHashTable(region, tuple_size);
}

void OpJoinHashTableBuild(tpl::sql::JoinHashTable *join_hash_table) {
  join_hash_table->Build();
}

void OpJoinHashTableFree(tpl::sql::JoinHashTable *join_hash_table) {
  join_hash_table->~JoinHashTable();
}

// ---------------------------------------------------------
// Aggregation Hash Table
// ---------------------------------------------------------

void OpAggregationHashTableInit(tpl::sql::AggregationHashTable *agg_table,
                                tpl::util::Region *region, u32 entry_size) {
  new (agg_table) tpl::sql::AggregationHashTable(region, entry_size);
}

void OpAggregationHashTableFree(tpl::sql::AggregationHashTable *agg_table) {
  agg_table->~AggregationHashTable();
}

}  //
