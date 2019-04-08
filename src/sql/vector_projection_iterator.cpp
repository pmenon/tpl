#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

VectorProjectionIterator::VectorProjectionIterator()
    : vector_projection_(nullptr),
      curr_idx_(0),
      num_selected_(0),
      selection_vector_{0},
      selection_vector_read_idx_(0),
      selection_vector_write_idx_(0) {
  selection_vector_[0] = VectorProjectionIterator::kInvalidPos;
}

VectorProjectionIterator::VectorProjectionIterator(VectorProjection *vp)
    : VectorProjectionIterator() {
  SetVectorProjection(vp);
}

void VectorProjectionIterator::SetVectorProjection(VectorProjection *vp) {
  vector_projection_ = vp;
  num_selected_ = vp->total_tuple_count();
  curr_idx_ = 0;
  selection_vector_[0] = kInvalidPos;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

}  // namespace tpl::sql
