#pragma once

#include "common/exception.h"
#include "sql/vector.h"
#include "sql/vector_operations/traits.h"
#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

/**
 * Check:
 * - Input and output vectors have the same type.
 * - Input and output vectors have the same shape.
 *
 * @param result The vector storing the result of the in-place operation.
 * @param input The right-side input into the in-place operation.
 */
inline void CheckInplaceOperation(const Vector *result, const Vector &input) {
  if (result->GetTypeId() != input.GetTypeId()) {
    throw TypeMismatchException(
        result->GetTypeId(), input.GetTypeId(),
        "left and right vector types to inplace operation must be the same");
  }
  if (!input.IsConstant() && result->GetCount() != input.GetCount()) {
    throw Exception(ExceptionType::Cardinality,
                    "left and right input vectors to binary operation must have the same size");
  }
}

/**
 * Helper function to execute an arithmetic in-place operation on two vectors and store the result
 * into the first input/output vector. The operations are performed only on the active elements in
 * the input vectors. It is assumed that the input vectors have the same size and selection vectors
 * (if any).
 *
 * This function leverages the tpl::sql::traits::ShouldPerformFullCompute trait to determine whether
 * the operation should be performed on ALL vector elements or just the active elements. Users can
 * control this feature by optionally specialization the trait for their operation type.
 *
 * @pre Both input vectors must have the same shape!
 *
 * @tparam ResultType The native CPP type of the elements in the result output vector.
 * @tparam InputType The native CPP type of the elements in the first input vector.
 * @tparam Op The binary operation to perform. Each invocation will receive an element from the
 *            result and input input vectors and must produce an element that is stored back into
 *            the result vector.
 * @param[in,out] result The result vector.
 * @param input The right input.
 */
template <typename ResultType, typename InputType, class Op>
void TemplatedInPlaceOperation(Vector *result, const Vector &input) {
  auto input_data = reinterpret_cast<InputType *>(input.GetData());
  auto result_data = reinterpret_cast<ResultType *>(result->GetData());

  if (input.IsConstant()) {
    if (input.IsNull(0)) {
      result->GetMutableNullMask()->SetAll();
    } else {
      if (traits::ShouldPerformFullCompute<InputType, Op>()(result->GetSelectionVector())) {
        VectorOps::ExecIgnoreFilter(
            *result, [&](uint64_t i, uint64_t k) { Op::Apply(&result_data[i], input_data[0]); });
      } else {
        VectorOps::Exec(*result,
                        [&](uint64_t i, uint64_t k) { Op::Apply(&result_data[i], input_data[0]); });
      }
    }
  } else {
    TPL_ASSERT(result->GetSelectionVector() == input.GetSelectionVector(),
               "Filter list of inputs to in-place operation do not match");

    result->GetMutableNullMask()->Union(input.GetNullMask());
    if (traits::ShouldPerformFullCompute<InputType, Op>()(result->GetSelectionVector())) {
      VectorOps::ExecIgnoreFilter(
          *result, [&](uint64_t i, uint64_t k) { Op::Apply(&result_data[i], input_data[i]); });
    } else {
      VectorOps::Exec(*result,
                      [&](uint64_t i, uint64_t k) { Op::Apply(&result_data[i], input_data[i]); });
    }
  }
}

}  // namespace tpl::sql
