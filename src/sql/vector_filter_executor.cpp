#include "sql/vector_filter_executor.h"

#include <vector>

#include "llvm/ADT/SmallVector.h"

#include "sql/constant_vector.h"
#include "sql/scoped_selection.h"
#include "sql/vector_operations/vector_operators.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/vector_util.h"

namespace tpl::sql {

VectorFilterExecutor::VectorFilterExecutor(VectorProjection *vector_projection)
    : vector_projection_(vector_projection),
      sel_vector_(vector_projection_->GetSelectionVector()),
      owned_sel_vector_{0},
      count_(vector_projection_->GetTupleCount()) {}

VectorFilterExecutor::VectorFilterExecutor(
    VectorProjectionIterator *vector_projection_iterator)
    : VectorFilterExecutor(vector_projection_iterator->GetVectorProjection()) {}

template <typename F>
void VectorFilterExecutor::SelectInternal(const u32 col_indexes[],
                                          const u32 num_cols, F &&filter) {
  // Collect input vectors
  llvm::SmallVector<Vector *, 8> inputs(num_cols);
  for (u64 i = 0; i < num_cols; i++) {
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

void VectorFilterExecutor::SelectGeneric(
    const std::vector<u32> &col_indexes,
    const VectorFilterExecutor::VectorFilterFn &filter) {
  SelectInternal(col_indexes.data(), col_indexes.size(), filter);
}

void VectorFilterExecutor::SelectEqVal(const u32 col_idx,
                                       const GenericValue &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectEqual(*inputs[0],
                                                 ConstantVector(val), output);
                 });
}

void VectorFilterExecutor::SelectEqVal(u32 col_idx, const Val &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   const auto generic_val =
                       GenericValue::CreateFromRuntimeValue(
                           inputs[0]->type_id(), val);
                   return VectorOps::SelectEqual(
                       *inputs[0], ConstantVector(generic_val), output);
                 });
}

void VectorFilterExecutor::SelectGeVal(const u32 col_idx,
                                       const GenericValue &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectGreaterThanEqual(
                       *inputs[0], ConstantVector(val), output);
                 });
}

void VectorFilterExecutor::SelectGeVal(u32 col_idx, const Val &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   const auto generic_val =
                       GenericValue::CreateFromRuntimeValue(
                           inputs[0]->type_id(), val);
                   return VectorOps::SelectGreaterThanEqual(
                       *inputs[0], ConstantVector(generic_val), output);
                 });
}

void VectorFilterExecutor::SelectGtVal(const u32 col_idx,
                                       const GenericValue &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectGreaterThan(
                       *inputs[0], ConstantVector(val), output);
                 });
}

void VectorFilterExecutor::SelectGtVal(u32 col_idx, const Val &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   const auto generic_val =
                       GenericValue::CreateFromRuntimeValue(
                           inputs[0]->type_id(), val);
                   return VectorOps::SelectGreaterThan(
                       *inputs[0], ConstantVector(generic_val), output);
                 });
}

void VectorFilterExecutor::SelectLeVal(const u32 col_idx,
                                       const GenericValue &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectLessThanEqual(
                       *inputs[0], ConstantVector(val), output);
                 });
}

void VectorFilterExecutor::SelectLeVal(u32 col_idx, const Val &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   const auto generic_val =
                       GenericValue::CreateFromRuntimeValue(
                           inputs[0]->type_id(), val);
                   return VectorOps::SelectLessThanEqual(
                       *inputs[0], ConstantVector(generic_val), output);
                 });
}

void VectorFilterExecutor::SelectLtVal(const u32 col_idx,
                                       const GenericValue &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectLessThan(
                       *inputs[0], ConstantVector(val), output);
                 });
}

void VectorFilterExecutor::SelectLtVal(u32 col_idx, const Val &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   const auto generic_val =
                       GenericValue::CreateFromRuntimeValue(
                           inputs[0]->type_id(), val);
                   return VectorOps::SelectLessThan(
                       *inputs[0], ConstantVector(generic_val), output);
                 });
}

void VectorFilterExecutor::SelectNeVal(const u32 col_idx,
                                       const GenericValue &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectNotEqual(
                       *inputs[0], ConstantVector(val), output);
                 });
}

void VectorFilterExecutor::SelectNeVal(u32 col_idx, const Val &val) {
  const u32 col_indexes[1] = {col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [&val](const Vector *inputs[], sel_t output[]) {
                   const auto generic_val =
                       GenericValue::CreateFromRuntimeValue(
                           inputs[0]->type_id(), val);
                   return VectorOps::SelectNotEqual(
                       *inputs[0], ConstantVector(generic_val), output);
                 });
}

void VectorFilterExecutor::SelectEq(const u32 left_col_idx,
                                    const u32 right_col_idx) {
  const u32 col_indexes[2] = {left_col_idx, right_col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectEqual(*inputs[0], *inputs[1],
                                                 output);
                 });
}

void VectorFilterExecutor::SelectGe(const u32 left_col_idx,
                                    const u32 right_col_idx) {
  const u32 col_indexes[2] = {left_col_idx, right_col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectGreaterThanEqual(*inputs[0],
                                                            *inputs[1], output);
                 });
}

void VectorFilterExecutor::SelectGt(const u32 left_col_idx,
                                    const u32 right_col_idx) {
  const u32 col_indexes[2] = {left_col_idx, right_col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectGreaterThan(*inputs[0], *inputs[1],
                                                       output);
                 });
}

void VectorFilterExecutor::SelectLe(const u32 left_col_idx,
                                    const u32 right_col_idx) {
  const u32 col_indexes[2] = {left_col_idx, right_col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectLessThanEqual(*inputs[0], *inputs[1],
                                                         output);
                 });
}

void VectorFilterExecutor::SelectLt(const u32 left_col_idx,
                                    const u32 right_col_idx) {
  const u32 col_indexes[2] = {left_col_idx, right_col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectLessThan(*inputs[0], *inputs[1],
                                                    output);
                 });
}

void VectorFilterExecutor::SelectNe(const u32 left_col_idx,
                                    const u32 right_col_idx) {
  const u32 col_indexes[2] = {left_col_idx, right_col_idx};
  SelectInternal(col_indexes, sizeof(col_indexes) / sizeof(col_indexes[0]),
                 [](const Vector *inputs[], sel_t output[]) {
                   return VectorOps::SelectNotEqual(*inputs[0], *inputs[1],
                                                    output);
                 });
}

void VectorFilterExecutor::InvertSelection() {
  u8 scratch[kDefaultVectorSize];
  util::VectorUtil::DiffSelected(kDefaultVectorSize, sel_vector_, count_,
                                 owned_sel_vector_, &count_, scratch);
  sel_vector_ = owned_sel_vector_;
}

void VectorFilterExecutor::Finish() {
  vector_projection_->SetSelectionVector(owned_sel_vector_, count_);
}

}  // namespace tpl::sql
