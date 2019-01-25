#include "sql/column_vector_iterator.h"

#include "sql/column.h"

namespace tpl::sql {

ColumnVectorIterator::ColumnVectorIterator(
    const Schema::ColumnInfo &col_info) noexcept
    : col_info_(col_info),
      column_(nullptr),
      curr_block_pos_(0),
      next_block_pos_(0),
      col_data_(nullptr),
      col_null_bitmap_(nullptr) {}

bool ColumnVectorIterator::Advance() {
  if (column() == nullptr || next_block_pos() == column()->num_tuples()) {
    return false;
  }

  u32 next_elem_offset = next_block_pos() * col_info().StorageSize();

  col_data_ = const_cast<byte *>(column()->AccessRaw(next_elem_offset));
  col_null_bitmap_ = const_cast<u32 *>(column()->AccessRawNullBitmap(0));

  curr_block_pos_ = next_block_pos();
  next_block_pos_ = std::min(column()->num_tuples(),
                             current_block_pos() + kDefaultVectorSize);

  return true;
}

void ColumnVectorIterator::Reset(const ColumnVector *column) {
  TPL_ASSERT(column != nullptr, "Cannot reset iterator with NULL block");
  column_ = column;
  col_data_ = const_cast<byte *>(column->AccessRaw(0));
  col_null_bitmap_ = const_cast<u32 *>(column->AccessRawNullBitmap(0));
  curr_block_pos_ = 0;
  next_block_pos_ = std::min(column->num_tuples(), kDefaultVectorSize);
  ;
}

}  // namespace tpl::sql
