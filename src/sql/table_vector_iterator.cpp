#include "sql/table_vector_iterator.h"

#include "logging/logger.h"

namespace tpl::sql {

TableVectorIterator::TableVectorIterator(const Table &table)
    : block_iterator_(table.Iterate()),
      vector_projection_(table.num_columns(), kDefaultVectorSize) {
  TPL_ASSERT(table.num_columns() > 0, "Cannot scan table with no columns");

  // Reserve space for the column iterators
  column_iterators_.reserve(table.num_columns());

  // Set up each column iterator for the columns we'll iterate over
  for (u32 col_idx = 0; col_idx < table.num_columns(); col_idx++) {
    const Schema::ColumnInfo &col_info = table.schema().GetColumnInfo(col_idx);
    column_iterators_.emplace_back(col_info);
  }
}

void TableVectorIterator::RefreshVectorProjection() {
  //
  // Setup the column's data in the vector projection with new data from the
  // column iterators
  //

  for (u32 col_idx = 0; col_idx < column_iterators_.size(); col_idx++) {
    vector_projection_.ResetColumn(column_iterators_, col_idx);
  }

  // Insert our vector projection instance into the vector projection iterator
  vector_projection_iterator_.SetVectorProjection(&vector_projection_);
}

bool TableVectorIterator::Advance() {
  //
  // First, we try to advance all the column iterators. We issue Advance()
  // calls to **all** column iterators to make sure they're consistent. If we're
  // able to advance all the column iterators, then we're certain there is
  // another vector of input; in this case, we just need to set up the
  // vector projection iterator with the new data and finish.
  //
  // Typically, either all column iterators can advance or non advance. If any
  // one of the column iterators says they're out of data, we advance the
  // table's block iterator looking for another block of input data. If there is
  // another block, we refresh the column iterators with the new block and
  // notify the vector projection of the new column data.
  //
  // If we are unable to advance the column iterators and the table's block
  // iterator, there isn't any more data to iterate over.
  //

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
    const Table::Block *block = block_iterator_.current_block();
    for (u32 i = 0; i < column_iterators_.size(); i++) {
      const ColumnVector *col = block->GetColumnData(i);
      column_iterators_[i].Reset(col);
    }
    RefreshVectorProjection();
    return true;
  }

  return false;
}

void TableVectorIterator::Reset(const Table &table) {
  // Reset the block iterator.
  block_iterator_ = table.Iterate();
  if (block_iterator_.Advance()) {
    // Reset each column iterator.
    const Table::Block *block = block_iterator_.current_block();
    for (u32 i = 0; i < column_iterators_.size(); i++) {
      const ColumnVector *col = block->GetColumnData(i);
      column_iterators_[i].Reset(col);
    }
    RefreshVectorProjection();
  }
}

}  // namespace tpl::sql
