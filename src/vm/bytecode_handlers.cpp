#include "vm/bytecode_handlers.h"

#include "sql/catalog.h"

extern "C" {

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorInit(tpl::sql::TableVectorIterator *iter, uint16_t table_id) {
  TPL_ASSERT(iter != nullptr, "Null iterator to initialize");
  new (iter) tpl::sql::TableVectorIterator(table_id);
}

void OpTableVectorIteratorPerformInit(tpl::sql::TableVectorIterator *iter) {
  TPL_ASSERT(iter != nullptr, "NULL iterator given to close");
  iter->Init();
}

void OpTableVectorIteratorFree(tpl::sql::TableVectorIterator *iter) {
  TPL_ASSERT(iter != nullptr, "NULL iterator given to close");
  iter->~TableVectorIterator();
}

void OpVPIInit(tpl::sql::VectorProjectionIterator *vpi, tpl::sql::VectorProjection *vp) {
  new (vpi) tpl::sql::VectorProjectionIterator(vp);
}

void OpVPIInitWithList(tpl::sql::VectorProjectionIterator *vpi, tpl::sql::VectorProjection *vp,
                       tpl::sql::TupleIdList *tid_list) {
  new (vpi) tpl::sql::VectorProjectionIterator(vp, tid_list);
}

void OpVPIFree(tpl::sql::VectorProjectionIterator *vpi) { vpi->~VectorProjectionIterator(); }

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

void OpFilterManagerInit(tpl::sql::FilterManager *filter_manager) {
  new (filter_manager) tpl::sql::FilterManager();
}

void OpFilterManagerStartNewClause(tpl::sql::FilterManager *filter_manager) {
  filter_manager->StartNewClause();
}

void OpFilterManagerInsertFilter(tpl::sql::FilterManager *filter_manager,
                                 tpl::sql::FilterManager::MatchFn clause) {
  filter_manager->InsertClauseTerm(clause);
}

void OpFilterManagerRunFilters(tpl::sql::FilterManager *filter_manager,
                               tpl::sql::VectorProjectionIterator *vpi) {
  filter_manager->RunFilters(vpi);
}

void OpFilterManagerFree(tpl::sql::FilterManager *filter_manager) {
  filter_manager->~FilterManager();
}

// ---------------------------------------------------------
// Join Hash Table
// ---------------------------------------------------------

void OpJoinHashTableInit(tpl::sql::JoinHashTable *join_hash_table, tpl::sql::MemoryPool *memory,
                         uint32_t tuple_size) {
  new (join_hash_table) tpl::sql::JoinHashTable(memory, tuple_size);
}

void OpJoinHashTableInit2(tpl::sql::JoinHashTable *join_hash_table, tpl::sql::MemoryPool *memory,
                          uint32_t tuple_size, tpl::sql::JoinHashTable::AnalysisPass analysis_fn,
                          tpl::sql::JoinHashTable::CompressPass compress_fn) {
  new (join_hash_table)
      tpl::sql::JoinHashTable(memory, tuple_size, false, analysis_fn, compress_fn);
}

void OpJoinHashTableBuild(tpl::sql::JoinHashTable *join_hash_table) { join_hash_table->Build(); }

void OpJoinHashTableBuildParallel(tpl::sql::JoinHashTable *join_hash_table,
                                  tpl::sql::ThreadStateContainer *thread_state_container,
                                  uint32_t jht_offset) {
  join_hash_table->MergeParallel(thread_state_container, jht_offset);
}

void OpJoinHashTableFree(tpl::sql::JoinHashTable *join_hash_table) {
  join_hash_table->~JoinHashTable();
}

// ---------------------------------------------------------
// Bit packing.
// ---------------------------------------------------------

void OpAnalysisStatsSetColumnCount(tpl::sql::JoinHashTable::AnalysisStats *stats,
                                   uint32_t column_count) {
  stats->SetNumCols(column_count);
}

void OpAnalysisStatsSetColumnBits(tpl::sql::JoinHashTable::AnalysisStats *stats, uint32_t column,
                                  uint32_t bits) {
  stats->SetBitsForCol(column, bits);
}

// ---------------------------------------------------------
// Aggregation Hash Table
// ---------------------------------------------------------

void OpAggregationHashTableInit(tpl::sql::AggregationHashTable *const agg_hash_table,
                                tpl::sql::MemoryPool *const memory, const uint32_t payload_size) {
  new (agg_hash_table) tpl::sql::AggregationHashTable(memory, payload_size);
}

void OpAggregationHashTableFree(tpl::sql::AggregationHashTable *const agg_hash_table) {
  agg_hash_table->~AggregationHashTable();
}

void OpAggregationHashTableIteratorInit(tpl::sql::AHTIterator *iter,
                                        tpl::sql::AggregationHashTable *agg_hash_table) {
  TPL_ASSERT(agg_hash_table != nullptr, "Null hash table");
  new (iter) tpl::sql::AHTIterator(*agg_hash_table);
}

void OpAggregationHashTableBuildAllHashTablePartitions(
    tpl::sql::AggregationHashTable *agg_hash_table, void *query_state) {
  agg_hash_table->BuildAllPartitions(query_state);
}

void OpAggregationHashTableRepartition(tpl::sql::AggregationHashTable *agg_hash_table) {
  agg_hash_table->Repartition();
}

void OpAggregationHashTableMergePartitions(
    tpl::sql::AggregationHashTable *agg_hash_table,
    tpl::sql::AggregationHashTable *target_agg_hash_table, void *query_state,
    tpl::sql::AggregationHashTable::MergePartitionFn merge_partition_fn) {
  agg_hash_table->MergePartitions(target_agg_hash_table, query_state, merge_partition_fn);
}

void OpAggregationHashTableIteratorFree(tpl::sql::AHTIterator *iter) { iter->~AHTIterator(); }

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

void OpSorterInit(tpl::sql::Sorter *const sorter, tpl::sql::MemoryPool *const memory,
                  const tpl::sql::Sorter::ComparisonFunction cmp_fn, const uint32_t tuple_size) {
  new (sorter) tpl::sql::Sorter(memory, cmp_fn, tuple_size);
}

void OpSorterSort(tpl::sql::Sorter *sorter) { sorter->Sort(); }

void OpSorterSortParallel(tpl::sql::Sorter *sorter,
                          tpl::sql::ThreadStateContainer *thread_state_container,
                          uint32_t sorter_offset) {
  sorter->SortParallel(thread_state_container, sorter_offset);
}

void OpSorterSortTopKParallel(tpl::sql::Sorter *sorter,
                              tpl::sql::ThreadStateContainer *thread_state_container,
                              uint32_t sorter_offset, uint64_t top_k) {
  sorter->SortTopKParallel(thread_state_container, sorter_offset, top_k);
}

void OpSorterFree(tpl::sql::Sorter *sorter) { sorter->~Sorter(); }

void OpSorterIteratorInit(tpl::sql::SorterIterator *iter, tpl::sql::Sorter *sorter) {
  new (iter) tpl::sql::SorterIterator(*sorter);
}

void OpSorterIteratorFree(tpl::sql::SorterIterator *iter) { iter->~SorterIterator(); }

// ---------------------------------------------------------
// CSV Reader
// ---------------------------------------------------------

void OpCSVReaderInit(tpl::util::CSVReader *reader, const uint8_t *file_name, uint32_t len) {
  std::string_view fname(reinterpret_cast<const char *>(file_name), len);
  new (reader) tpl::util::CSVReader(std::make_unique<tpl::util::CSVFile>(fname));
}

void OpCSVReaderPerformInit(bool *result, tpl::util::CSVReader *reader) {
  *result = reader->Initialize();
}

void OpCSVReaderClose(tpl::util::CSVReader *reader) { std::destroy_at(reader); }

}  //
