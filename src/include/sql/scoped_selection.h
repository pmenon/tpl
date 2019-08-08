#pragma once

#include <vector>

#include "sql/vector.h"

namespace tpl::sql {

/**
 * Scope class to temporarily and atomically switch selected/visible tuples for
 * a collection of input vectors. The original selection vector is switched back
 * into all input vectors when this class goes out of scope and is destroyed.
 */
class SelectionScope {
 public:
  SelectionScope(sel_t *sel_vector, u32 count,
                 const std::vector<Vector *> &vectors)
      : vectors_(vectors) {
    old_sel_vector_ = vectors_[0]->selection_vector();
    old_count_ = vectors_[0]->count();
    for (auto *vector : vectors_) {
#ifndef NDEBUG
      TPL_ASSERT(vector->selection_vector() == old_sel_vector_ &&
                     vector->count() == old_count_,
                 "All vectors must have the same selection vector and count");
#endif
      vector->SetSelectionVector(sel_vector, count);
    }
  }

  ~SelectionScope() {
    for (auto *vector : vectors_) {
      vector->SetSelectionVector(old_sel_vector_, old_count_);
    }
  }

 private:
  // The list of vectors we're controlling
  const std::vector<Vector *> &vectors_;
  // The previous selection vector
  sel_t *old_sel_vector_;
  // The previous count of the selection vector, or the original size of the
  // input vector.
  u32 old_count_;
};

}  // namespace tpl::sql
