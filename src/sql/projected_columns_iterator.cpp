#include "sql/projected_columns_iterator.h"

namespace tpl::sql {

ProjectedColumnsIterator::ProjectedColumnsIterator()
    : projected_column_(nullptr),
      curr_idx_(0),
      num_selected_(0),
      selection_vector_{0},
      selection_vector_read_idx_(0),
      selection_vector_write_idx_(0) {
  selection_vector_[0] = ProjectedColumnsIterator::kInvalidPos;
}

ProjectedColumnsIterator::ProjectedColumnsIterator(
    storage::ProjectedColumns *projected_column)
    : ProjectedColumnsIterator() {
  SetProjectedColumn(projected_column);
}

void ProjectedColumnsIterator::SetProjectedColumn(
    storage::ProjectedColumns *projected_column) {
  projected_column_ = projected_column;
  num_selected_ = projected_column_->NumTuples();
  curr_idx_ = 0;
  selection_vector_[0] = kInvalidPos;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

}  // namespace tpl::sql
