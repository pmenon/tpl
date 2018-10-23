#include "sql/table.h"

namespace tpl::sql {

void Table::BulkInsert(std::vector<Table::ColumnVector> &&data, u32 num_rows) {
  blocks_.emplace_back(std::move(data), num_rows);
}

bool Table::Scan(TableIterator *iter) const {
  if (iter->block_ == blocks_.size()) {
    return false;
  }

  // The columns in the new block
  const auto &block = blocks_[iter->block_];
  const auto &cols = block.columns;

  iter->cols_.resize(cols.size());
  for (u32 i = 0; i < cols.size(); i++) {
    iter->cols_[i] = &cols[i];
  }
  iter->block_ += 1;
  iter->pos_ = 0;
  iter->bound_ = block.num_rows;
  return true;
}

bool TableIterator::Next() {
  if (++pos_ >= bound_) {
    return table_->Scan(this);
  }
  return true;
}

}  // namespace tpl::sql