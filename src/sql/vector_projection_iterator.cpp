#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

VectorProjectionIterator::VectorProjectionIterator()
    : vp_(nullptr),
      curr_pos_(0),
      num_selected_(0),
      selection_vector_{0},
      selection_vector_read_pos_(0),
      selection_vector_write_pos_(0) {
  selection_vector()[0] = VectorProjectionIterator::kInvalidPos;
}

VectorProjectionIterator::VectorProjectionIterator(VectorProjection *vp)
    : VectorProjectionIterator() {
  SetVectorProjection(vp);
}

}  // namespace tpl::sql
