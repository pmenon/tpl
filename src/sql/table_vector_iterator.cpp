#include "sql/table_vector_iterator.h"

#include <limits>
#include <numeric>
#include <utility>
#include <vector>

#include "tbb/parallel_for.h"

#include "logging/logger.h"
#include "sql/catalog.h"
#include "sql/thread_state_container.h"
#include "util/timer.h"

namespace tpl::sql {

// Iterate over the table and select all columns
TableVectorIterator::TableVectorIterator(const uint16_t table_id)
    : TableVectorIterator(table_id, 0, std::numeric_limits<uint32_t>::max(), {}) {}

// Iterate over a subset of the table and select all columns
TableVectorIterator::TableVectorIterator(uint16_t table_id, uint32_t start_block_idx,
                                         uint32_t end_block_idx)
    : TableVectorIterator(table_id, start_block_idx, end_block_idx, {}) {}

// Iterate over the table, but only select the given columns
TableVectorIterator::TableVectorIterator(const uint16_t table_id,
                                         std::vector<uint32_t> column_indexes)
    : TableVectorIterator(table_id, 0, std::numeric_limits<uint32_t>::max(),
                          std::move(column_indexes)) {}

TableVectorIterator::TableVectorIterator(uint16_t table_id, uint32_t start_block_idx,
                                         uint32_t end_block_idx,
                                         std::vector<uint32_t> column_indexes)
    : column_indexes_(std::move(column_indexes)),
      block_iterator_(table_id, start_block_idx, end_block_idx),
      initialized_(false) {}

bool TableVectorIterator::Init() {
  // No-op if already initialized
  if (IsInitialized()) {
    return true;
  }

  // If we can't initialize the block iterator, fail
  if (!block_iterator_.Init()) {
    return false;
  }

  // The table schema
  const auto &table_schema = block_iterator_.GetTable()->GetSchema();

  // If the column indexes vector is empty, select all the columns
  if (column_indexes_.empty()) {
    column_indexes_.resize(table_schema.GetColumnCount());
    std::iota(column_indexes_.begin(), column_indexes_.end(), uint32_t{0});
  }

  // Collect column metadata for the iterators
  std::vector<const Schema::ColumnInfo *> col_infos(column_indexes_.size());
  for (uint64_t idx = 0; idx < column_indexes_.size(); idx++) {
    col_infos[idx] = table_schema.GetColumnInfo(idx);
  }

  // Configure the vector projection
  std::vector<TypeId> col_types(column_indexes_.size());
  for (uint64_t idx = 0; idx < column_indexes_.size(); idx++) {
    col_types[idx] = col_infos[idx]->type.GetPrimitiveTypeId();
  }
  vector_projection_.InitializeEmpty(col_types);

  // Create the column iterators
  column_iterators_.reserve(col_infos.size());
  for (const auto *col_info : col_infos) {
    column_iterators_.emplace_back(col_info);
  }

  // All good
  initialized_ = true;
  return true;
}

void TableVectorIterator::RefreshVectorProjection() {
  // Reset our projection and refresh all columns with new data from the column
  // iterators.

  const uint32_t tuple_count = column_iterators_[0].GetTupleCount();

  TPL_ASSERT(std::all_of(column_iterators_.begin(), column_iterators_.end(),
                         [&](const auto &iter) { return tuple_count == iter.GetTupleCount(); }),
             "Not all iterators have the same size?");

  vector_projection_.Reset(tuple_count);
  for (uint64_t col_idx = 0; col_idx < column_iterators_.size(); col_idx++) {
    Vector *column_vector = vector_projection_.GetColumn(col_idx);
    column_vector->Reference(column_iterators_[col_idx].GetColumnData(),
                             column_iterators_[col_idx].GetColumnNullBitmap(),
                             column_iterators_[col_idx].GetTupleCount());
  }
  vector_projection_.CheckIntegrity();

  // Insert our vector projection instance into the vector projection iterator
  vector_projection_iterator_.SetVectorProjection(&vector_projection_);
}

bool TableVectorIterator::Advance() {
  // Cannot advance if not initialized
  if (!IsInitialized()) {
    return false;
  }

  // First, try to advance ALL the column iterators. If all column iterators
  // successfully advance, we set up the vector projection with the new column
  // data, and reset the vector projection iterator. Normally, either all column
  // iterators advance or non advance. It is a more serious error if a partial
  // subset of iterators advance.
  //
  // If any one of the column iterators indicate that they've exhausted data in
  // their segment, we advance the table's block iterator looking for another
  // block of input data. If there is another block, we refresh the column
  // iterators with the new block and notify the vector projection of the new
  // column data.
  //
  // If we are unable to advance both the column AND block iterators, there
  // isn't any more data to iterate over.

  bool advanced = true;
  for (auto &col_iter : column_iterators_) {
    advanced &= col_iter.Advance();
  }

  if (advanced) {
    RefreshVectorProjection();
    return true;
  }

  // Check block iterator
  if (block_iterator_.Advance()) {
    const Table::Block *block = block_iterator_.GetCurrentBlock();
    for (uint64_t i = 0; i < column_iterators_.size(); i++) {
      const ColumnSegment *col = block->GetColumnData(i);
      column_iterators_[i].Reset(col);
    }
    RefreshVectorProjection();
    return true;
  }

  return false;
}

namespace {

class ScanTask {
 public:
  ScanTask(uint16_t table_id, void *const query_state,
           ThreadStateContainer *const thread_state_container, TableVectorIterator::ScanFn scanner)
      : table_id_(table_id),
        query_state_(query_state),
        thread_state_container_(thread_state_container),
        scanner_(scanner) {}

  void operator()(const tbb::blocked_range<uint32_t> &block_range) const {
    // Create the iterator over the specified block range
    TableVectorIterator iter(table_id_, block_range.begin(), block_range.end());

    // Initialize it
    if (!iter.Init()) {
      return;
    }

    // Pull out the thread-local state
    byte *const thread_state = thread_state_container_->AccessCurrentThreadState();

    // Call scanning function
    scanner_(query_state_, thread_state, &iter);
  }

 private:
  uint16_t table_id_;
  void *const query_state_;
  ThreadStateContainer *const thread_state_container_;
  TableVectorIterator::ScanFn scanner_;
};

}  // namespace

bool TableVectorIterator::ParallelScan(const uint16_t table_id, void *const query_state,
                                       ThreadStateContainer *const thread_states,
                                       const TableVectorIterator::ScanFn scan_fn,
                                       const uint32_t min_grain_size) {
  // Lookup table
  const Table *table = Catalog::Instance()->LookupTableById(table_id);
  if (table == nullptr) {
    return false;
  }

  // Time
  util::Timer<std::milli> timer;
  timer.Start();

  // Execute parallel scan
  tbb::blocked_range<uint32_t> block_range(0, table->GetBlockCount(), min_grain_size);
  tbb::parallel_for(block_range, ScanTask(table_id, query_state, thread_states, scan_fn));

  timer.Stop();

  double tps = table->GetTupleCount() / timer.GetElapsed() / 1000.0;
  LOG_DEBUG("Scanned {} blocks ({} tuples) in {} ms ({:.3f} mtps)", table->GetBlockCount(),
            table->GetTupleCount(), timer.GetElapsed(), tps);

  return true;
}

}  // namespace tpl::sql
