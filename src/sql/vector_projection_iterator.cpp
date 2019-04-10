#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

VectorProjectionIterator::VectorProjectionIterator()
    : projected_column_(nullptr),
      curr_idx_(0),
      num_selected_(0),
      selection_vector_{0},
      selection_vector_read_idx_(0),
      selection_vector_write_idx_(0) {
  selection_vector_[0] = VectorProjectionIterator::kInvalidPos;
}

VectorProjectionIterator::VectorProjectionIterator(
    storage::ProjectedColumns *projected_column)
    : VectorProjectionIterator() {
  SetProjectedColumn(projected_column);
}

void VectorProjectionIterator::SetProjectedColumn(
    storage::ProjectedColumns *projected_column) {
  projected_column_ = projected_column;
  num_selected_ = projected_column_->NumTuples();
  curr_idx_ = 0;
  selection_vector_[0] = kInvalidPos;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

}  // namespace tpl::sql
