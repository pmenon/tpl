#pragma once

#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

/**
 * Helper function for unary operations that want to handle NULLs specifically.
 * @tparam InputType The native CPP type of the values in the input vector.
 * @tparam ResultType The native CPP type of the values in the result vector.
 * @tparam Op The unary operation to perform on each element in the input.
 * @param input The input vector to read values from.
 * @param[out] result The output vector where the results of the unary operation
 *                    are written into. The result vector will have the same
 *                    selection vector and count as the input vector.
 */
template <typename InputType, typename ResultType, typename Op>
static inline void UnaryOperation_HandleNull(const Vector &input, Vector *result) {
  auto *input_data = reinterpret_cast<InputType *>(input.data());
  auto *result_data = reinterpret_cast<ResultType *>(result->data());

  auto null_mask = input.null_mask();
  result->set_null_mask(null_mask.reset());

  if (!input.null_mask().any()) {
    VectorOps::Exec(input, [&](u64 i, u64 k) { result_data[i] = Op::Apply(input_data[i], false); });
  } else {
    VectorOps::Exec(input, [&](u64 i, u64 k) {
      result_data[i] = Op::Apply(input_data[i], input.null_mask()[i]);
    });
  }

  result->SetSelectionVector(input.selection_vector(), input.count());
}

/**
 * Helper function to execute a unary function on all active elements in an
 * input vector and store the results into an output vector. The function is
 * evaluated on all active elements, including NULL elements.
 * @tparam InputType The native CPP type of the values in the input vector.
 * @tparam ResultType The native CPP type of the values in the result vector.
 * @tparam Op The unary operation to perform on each element in the input.
 * @param input The input vector to read values from.
 * @param result The output vector where the results of the unary operation
 *               are written into. The result vector will have the same
 *               selection vector and count as the input vector.
 */
template <typename InputType, typename ResultType, typename Op>
static inline void UnaryOperation(const Vector &input, Vector *result) {
  auto *input_data = reinterpret_cast<InputType *>(input.data());
  auto *result_data = reinterpret_cast<ResultType *>(result->data());

  VectorOps::Exec(input, [&](u64 i, u64 k) { result_data[i] = Op::Apply(input_data[i]); });

  result->set_null_mask(input.null_mask());
  result->SetSelectionVector(input.selection_vector(), input.count());
}

}  // namespace tpl::sql
