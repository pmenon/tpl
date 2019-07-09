#pragma once

#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

template <typename InputType, typename ResultType, typename Op>
static inline void TemplatedUnaryOperation(const Vector &input,
                                           Vector *result) {
  auto *input_data = reinterpret_cast<InputType *>(input.data());
  auto *result_data = reinterpret_cast<ResultType *>(result->data());

  VectorOps::Exec(
      input, [&](u64 i, u64 k) { result_data[i] = Op::Apply(input_data[i]); });

  result->set_null_mask(input.null_mask());
  result->SetSelectionVector(input.selection_vector(), input.count());
}

}  // namespace tpl::sql