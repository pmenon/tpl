#pragma once

#include "sql/vector.h"

namespace tpl::sql {

/**
 * Scope class to temporarily and atomically switch selected/visible tuples for
 * a collection of input vectors. The original selection vector is switched back
 * into all input vectors when this class goes out of scope and is destroyed.
 *
 * @tparam ContainerT The type of container storing all vector instances. Must
 *                    provide forward iteration.
 */
template <typename ContainerT>
class SelectionScope {
  static_assert(
      std::is_base_of_v<
          std::forward_iterator_tag,
          typename std::iterator_traits<typename ContainerT::const_iterator>::iterator_category>,
      "Container provided to SelectionScope must provide support for forward "
      "iteration");

 public:
  SelectionScope(sel_t *sel_vector, const uint32_t count, const ContainerT &vectors)
      : vectors_(vectors) {
    old_sel_vector_ = vectors_[0]->selection_vector();
    old_count_ = vectors_[0]->count();
    for (Vector *vector : vectors_) {
      TPL_ASSERT(vector->selection_vector() == old_sel_vector_,
                 "All vectors must have the same selection vector");
      TPL_ASSERT(vector->count() == old_count_, "All vectors must have the same count");
      vector->SetSelectionVector(sel_vector, count);
    }
  }

  ~SelectionScope() {
    for (Vector *vector : vectors_) {
      vector->SetSelectionVector(old_sel_vector_, old_count_);
    }
  }

 private:
  // The list of vectors we're controlling
  const ContainerT &vectors_;
  // The previous selection vector
  sel_t *old_sel_vector_;
  // The previous count of the selection vector, or the original size of the
  // input vector.
  uint32_t old_count_;
};

}  // namespace tpl::sql
