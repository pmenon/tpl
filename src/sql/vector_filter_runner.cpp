#include "sql/vector_filter_runner.h"

#include <vector>

#include "sql/constant_vector.h"
#include "sql/scoped_selection.h"
#include "sql/vector_operations/vector_operators.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/vector_util.h"

namespace tpl::sql {

VectorFilterRunner::VectorFilterRunner(VectorProjection *vector_projection)
    : vector_projection_(vector_projection),
      sel_vector_(vector_projection_->GetSelectionVector()),
      owned_sel_vector_{0},
      count_(vector_projection_->GetTupleCount()) {}

VectorFilterRunner::VectorFilterRunner(
    VectorProjectionIterator *vector_projection_iterator)
    : VectorFilterRunner(vector_projection_iterator->GetVectorProjection()) {}

void VectorFilterRunner::SelectGeneric(
    const std::vector<u32> &col_indexes,
    const VectorFilterRunner::VectorFilterFn &filter) {
  // Collect inputs
  std::vector<Vector *> inputs(col_indexes.size());
  for (u64 i = 0; i < col_indexes.size(); i++) {
    inputs[i] = vector_projection_->GetColumn(col_indexes[i]);
  }

  // Temporary selection visibility
  SelectionScope scoped_selection(sel_vector_, count_, inputs);

  // Apply function
  count_ =
      filter(const_cast<const Vector **>(inputs.data()), owned_sel_vector_);

  // Switch selections to output of filter
  sel_vector_ = owned_sel_vector_;
}

void VectorFilterRunner::SelectEqVal(const u32 col_idx,
                                     const GenericValue &val) {
  SelectGeneric({col_idx}, [&val](const Vector *vecs[], sel_t output[]) {
    return VectorOps::SelectEqual(*vecs[0], ConstantVector(val), output);
  });
}

void VectorFilterRunner::SelectGeVal(const u32 col_idx,
                                     const GenericValue &val) {
  SelectGeneric({col_idx}, [&val](const Vector *vecs[], sel_t output[]) {
    return VectorOps::SelectGreaterThanEqual(*vecs[0], ConstantVector(val),
                                             output);
  });
}

void VectorFilterRunner::SelectGtVal(const u32 col_idx,
                                     const GenericValue &val) {
  SelectGeneric({col_idx}, [&val](const Vector *vecs[], sel_t output[]) {
    return VectorOps::SelectGreaterThan(*vecs[0], ConstantVector(val), output);
  });
}

void VectorFilterRunner::SelectLeVal(const u32 col_idx,
                                     const GenericValue &val) {
  SelectGeneric({col_idx}, [&val](const Vector *vecs[], sel_t output[]) {
    return VectorOps::SelectLessThanEqual(*vecs[0], ConstantVector(val),
                                          output);
  });
}

void VectorFilterRunner::SelectLtVal(const u32 col_idx,
                                     const GenericValue &val) {
  SelectGeneric({col_idx}, [&val](const Vector *vecs[], sel_t output[]) {
    return VectorOps::SelectLessThan(*vecs[0], ConstantVector(val), output);
  });
}

void VectorFilterRunner::SelectNeVal(const u32 col_idx,
                                     const GenericValue &val) {
  SelectGeneric({col_idx}, [&val](const Vector *vecs[], sel_t output[]) {
    return VectorOps::SelectNotEqual(*vecs[0], ConstantVector(val), output);
  });
}

void VectorFilterRunner::SelectEq(const u32 left_col_idx,
                                  const u32 right_col_idx) {
  SelectGeneric({left_col_idx, right_col_idx},
                [](const Vector *vecs[], sel_t output[]) {
                  return VectorOps::SelectEqual(*vecs[0], *vecs[1], output);
                });
}

void VectorFilterRunner::SelectGe(const u32 left_col_idx,
                                  const u32 right_col_idx) {
  SelectGeneric(
      {left_col_idx, right_col_idx}, [](const Vector *vecs[], sel_t output[]) {
        return VectorOps::SelectGreaterThanEqual(*vecs[0], *vecs[1], output);
      });
}

void VectorFilterRunner::SelectGt(const u32 left_col_idx,
                                  const u32 right_col_idx) {
  SelectGeneric(
      {left_col_idx, right_col_idx}, [](const Vector *vecs[], sel_t output[]) {
        return VectorOps::SelectGreaterThan(*vecs[0], *vecs[1], output);
      });
}

void VectorFilterRunner::SelectLe(const u32 left_col_idx,
                                  const u32 right_col_idx) {
  SelectGeneric(
      {left_col_idx, right_col_idx}, [](const Vector *vecs[], sel_t output[]) {
        return VectorOps::SelectLessThanEqual(*vecs[0], *vecs[1], output);
      });
}

void VectorFilterRunner::SelectLt(const u32 left_col_idx,
                                  const u32 right_col_idx) {
  SelectGeneric({left_col_idx, right_col_idx},
                [](const Vector *vecs[], sel_t output[]) {
                  return VectorOps::SelectLessThan(*vecs[0], *vecs[1], output);
                });
}

void VectorFilterRunner::SelectNe(const u32 left_col_idx,
                                  const u32 right_col_idx) {
  SelectGeneric({left_col_idx, right_col_idx},
                [](const Vector *vecs[], sel_t output[]) {
                  return VectorOps::SelectNotEqual(*vecs[0], *vecs[1], output);
                });
}

void VectorFilterRunner::InvertSelection() {
  u8 scratch[kDefaultVectorSize];
  util::VectorUtil::DiffSelected(kDefaultVectorSize, sel_vector_, count_,
                                 owned_sel_vector_, &count_, scratch);
  sel_vector_ = owned_sel_vector_;
}

void VectorFilterRunner::Finish() {
  vector_projection_->SetSelectionVector(owned_sel_vector_, count_);
}

}  // namespace tpl::sql
