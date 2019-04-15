#include "vm/bytecode_handlers.h"

#include "sql/catalog.h"

extern "C" {

// ---------------------------------------------------------
// Region
// ---------------------------------------------------------

void OpRegionInit(tpl::util::Region *region) {
  new (region) tpl::util::Region("tmp");
}

void OpRegionFree(tpl::util::Region *region) { region->~Region(); }

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorInit(tpl::sql::TableVectorIterator *iter,
                               u16 table_id) {
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

// ---------------------------------------------------------
// VPI Vectorized Filters
// ---------------------------------------------------------

void OpVPIFilterEqual(u32 *size, tpl::sql::VectorProjectionIterator *iter,
                      u16 col_id, i64 val) {
  tpl::sql::VectorProjectionIterator::FilterVal v{.bi = val};
  *size = iter->FilterColByVal<std::equal_to>(col_id, v);
}

void OpVPIFilterGreaterThan(u32 *size, tpl::sql::VectorProjectionIterator *iter,
                            u16 col_id, i64 val) {
  tpl::sql::VectorProjectionIterator::FilterVal v{.bi = val};
  *size = iter->FilterColByVal<std::greater>(col_id, v);
}

void OpVPIFilterGreaterThanEqual(u32 *size,
                                 tpl::sql::VectorProjectionIterator *iter,
                                 u16 col_id, i64 val) {
  tpl::sql::VectorProjectionIterator::FilterVal v{.bi = val};
  *size = iter->FilterColByVal<std::greater_equal>(col_id, v);
}

void OpVPIFilterLessThan(u32 *size, tpl::sql::VectorProjectionIterator *iter,
                         u16 col_id, i64 val) {
  tpl::sql::VectorProjectionIterator::FilterVal v{.bi = val};
  *size = iter->FilterColByVal<std::less>(col_id, v);
}

void OpVPIFilterLessThanEqual(u32 *size,
                              tpl::sql::VectorProjectionIterator *iter,
                              u16 col_id, i64 val) {
  tpl::sql::VectorProjectionIterator::FilterVal v{.bi = val};
  *size = iter->FilterColByVal<std::less_equal>(col_id, v);
}

void OpVPIFilterNotEqual(u32 *size, tpl::sql::VectorProjectionIterator *iter,
                         u16 col_id, i64 val) {
  tpl::sql::VectorProjectionIterator::FilterVal v{.bi = val};
  *size = iter->FilterColByVal<std::not_equal_to>(col_id, v);
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

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

void OpSorterInit(tpl::sql::Sorter *sorter, tpl::util::Region *region,
                  tpl::sql::Sorter::ComparisonFunction cmp_fn, u32 tuple_size) {
  new (sorter) tpl::sql::Sorter(region, cmp_fn, tuple_size);
}

void OpSorterSort(tpl::sql::Sorter *sorter) { sorter->Sort(); }

void OpSorterFree(tpl::sql::Sorter *sorter) { sorter->~Sorter(); }

void OpSorterIteratorInit(tpl::sql::SorterIterator *iter,
                          tpl::sql::Sorter *sorter) {
  new (iter) tpl::sql::SorterIterator(sorter);
}

void OpSorterIteratorFree(tpl::sql::SorterIterator *iter) {
  iter->~SorterIterator();
}

}  //
