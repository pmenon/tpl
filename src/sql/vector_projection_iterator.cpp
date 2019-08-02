#include "sql/vector_projection_iterator.h"

#include "util/vector_util.h"

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
  num_selected_ = vp->GetTupleCount();
  curr_idx_ = 0;
  selection_vector_[0] = kInvalidPos;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

}  // namespace tpl::sql
