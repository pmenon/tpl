#include "sql/vectorized_iterator.h"

namespace tpl::sql {

VectorizedIterator::VectorizedIterator(const Table &table) noexcept
    : block_iterator_(table.Iterate()), row_batch_(2048) {}

bool VectorizedIterator::Next() noexcept {
  if (!block_iterator()->Next()) {
    return false;
  }

  SetupRowBatch(block_iterator()->current_block());
  return true;
}

void VectorizedIterator::SetupRowBatch(const Table::Block *block) {
  row_batch()->Reset(block->num_rows());

  for (u32 i = 0; i < block->num_cols(); i++) {
    row_batch()->AddColumn(block->GetColumnData(i));
  }
}

}  // namespace tpl::sql