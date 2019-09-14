#include "sql/vector_filter_executor.h"

#include <vector>

#include "llvm/ADT/SmallVector.h"

#include "sql/constant_vector.h"
#include "sql/vector_operations/vector_operators.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/vector_util.h"

namespace tpl::sql {

VectorFilterExecutor::VectorFilterExecutor(VectorProjection *vector_projection)
    : vector_projection_(vector_projection), tid_list_(vector_projection_->GetTotalTupleCount()) {
  if (const sel_t *sel_vector = vector_projection_->GetSelectionVector()) {
    tid_list_.BuildFromSelectionVector(sel_vector, vector_projection_->GetSelectedTupleCount());
  } else {
    tid_list_.AddAll();
  }
}

VectorFilterExecutor::VectorFilterExecutor(VectorProjectionIterator *vector_projection_iterator)
    : VectorFilterExecutor(vector_projection_iterator->GetVectorProjection()) {}

void VectorFilterExecutor::SelectGeneric(const std::vector<uint32_t> &col_indexes,
                                         const VectorFilterExecutor::VectorFilterFn &filter) {
  llvm::SmallVector<const Vector *, 8> vectors;
  for (const uint32_t col_idx : col_indexes) {
    vectors.push_back(vector_projection_->GetColumn(col_idx));
  }

  filter(vectors.data(), &tid_list_);
}

void VectorFilterExecutor::SelectEqVal(const uint32_t col_idx, const GenericValue &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  VectorOps::SelectEqual(*left_vector, ConstantVector(val), &tid_list_);
}

void VectorFilterExecutor::SelectEqVal(uint32_t col_idx, const Val &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  const GenericValue constant = GenericValue::CreateFromRuntimeValue(left_vector->type_id(), val);
  VectorOps::SelectEqual(*left_vector, ConstantVector(constant), &tid_list_);
}

void VectorFilterExecutor::SelectGeVal(const uint32_t col_idx, const GenericValue &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  VectorOps::SelectGreaterThanEqual(*left_vector, ConstantVector(val), &tid_list_);
}

void VectorFilterExecutor::SelectGeVal(uint32_t col_idx, const Val &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  const GenericValue constant = GenericValue::CreateFromRuntimeValue(left_vector->type_id(), val);
  VectorOps::SelectGreaterThanEqual(*left_vector, ConstantVector(constant), &tid_list_);
}

void VectorFilterExecutor::SelectGtVal(const uint32_t col_idx, const GenericValue &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  VectorOps::SelectGreaterThan(*left_vector, ConstantVector(val), &tid_list_);
}

void VectorFilterExecutor::SelectGtVal(uint32_t col_idx, const Val &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  const GenericValue constant = GenericValue::CreateFromRuntimeValue(left_vector->type_id(), val);
  VectorOps::SelectGreaterThan(*left_vector, ConstantVector(constant), &tid_list_);
}

void VectorFilterExecutor::SelectLeVal(const uint32_t col_idx, const GenericValue &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  VectorOps::SelectLessThanEqual(*left_vector, ConstantVector(val), &tid_list_);
}

void VectorFilterExecutor::SelectLeVal(uint32_t col_idx, const Val &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  const GenericValue constant = GenericValue::CreateFromRuntimeValue(left_vector->type_id(), val);
  VectorOps::SelectLessThanEqual(*left_vector, ConstantVector(constant), &tid_list_);
}

void VectorFilterExecutor::SelectLtVal(const uint32_t col_idx, const GenericValue &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  VectorOps::SelectLessThan(*left_vector, ConstantVector(val), &tid_list_);
}

void VectorFilterExecutor::SelectLtVal(uint32_t col_idx, const Val &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  const GenericValue constant = GenericValue::CreateFromRuntimeValue(left_vector->type_id(), val);
  VectorOps::SelectLessThan(*left_vector, ConstantVector(constant), &tid_list_);
}

void VectorFilterExecutor::SelectNeVal(const uint32_t col_idx, const GenericValue &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  VectorOps::SelectNotEqual(*left_vector, ConstantVector(val), &tid_list_);
}

void VectorFilterExecutor::SelectNeVal(uint32_t col_idx, const Val &val) {
  const Vector *left_vector = vector_projection_->GetColumn(col_idx);
  const GenericValue constant = GenericValue::CreateFromRuntimeValue(left_vector->type_id(), val);
  VectorOps::SelectNotEqual(*left_vector, ConstantVector(constant), &tid_list_);
}

void VectorFilterExecutor::SelectEq(const uint32_t left_col_idx, const uint32_t right_col_idx) {
  const Vector *left_vector = vector_projection_->GetColumn(left_col_idx);
  const Vector *right_vector = vector_projection_->GetColumn(right_col_idx);
  VectorOps::SelectEqual(*left_vector, *right_vector, &tid_list_);
}

void VectorFilterExecutor::SelectGe(const uint32_t left_col_idx, const uint32_t right_col_idx) {
  const Vector *left_vector = vector_projection_->GetColumn(left_col_idx);
  const Vector *right_vector = vector_projection_->GetColumn(right_col_idx);
  VectorOps::SelectGreaterThanEqual(*left_vector, *right_vector, &tid_list_);
}

void VectorFilterExecutor::SelectGt(const uint32_t left_col_idx, const uint32_t right_col_idx) {
  const Vector *left_vector = vector_projection_->GetColumn(left_col_idx);
  const Vector *right_vector = vector_projection_->GetColumn(right_col_idx);
  VectorOps::SelectGreaterThan(*left_vector, *right_vector, &tid_list_);
}

void VectorFilterExecutor::SelectLe(const uint32_t left_col_idx, const uint32_t right_col_idx) {
  const Vector *left_vector = vector_projection_->GetColumn(left_col_idx);
  const Vector *right_vector = vector_projection_->GetColumn(right_col_idx);
  VectorOps::SelectLessThanEqual(*left_vector, *right_vector, &tid_list_);
}

void VectorFilterExecutor::SelectLt(const uint32_t left_col_idx, const uint32_t right_col_idx) {
  const Vector *left_vector = vector_projection_->GetColumn(left_col_idx);
  const Vector *right_vector = vector_projection_->GetColumn(right_col_idx);
  VectorOps::SelectLessThan(*left_vector, *right_vector, &tid_list_);
}

void VectorFilterExecutor::SelectNe(const uint32_t left_col_idx, const uint32_t right_col_idx) {
  const Vector *left_vector = vector_projection_->GetColumn(left_col_idx);
  const Vector *right_vector = vector_projection_->GetColumn(right_col_idx);
  VectorOps::SelectNotEqual(*left_vector, *right_vector, &tid_list_);
}

void VectorFilterExecutor::Finish() {
  sel_t sel_vec[kDefaultVectorSize];
  uint32_t count = tid_list_.AsSelectionVector(sel_vec);
  vector_projection_->SetSelectionVector(sel_vec, count);
}

}  // namespace tpl::sql
