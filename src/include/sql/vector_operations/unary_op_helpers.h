#pragma once

#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

/**
 * Helper function for unary operations that want to handle NULLs specifically. The primary
 * difference between this function and @em UnaryOperation() below is that the templated @em Op
 * functor also accepts the boolean NULL indicator flag for each input element.
 * @tparam InputType The native CPP type of the values in the input vector.
 * @tparam ResultType The native CPP type of the values in the result vector.
 * @tparam Op The unary operation to perform on each element in the input. Receives both the input
 *            value and boolean NULL indicator flag.
 * @param input The input vector to read values from.
 * @param[out] result The output vector where the results of the unary operation are written into.
 *                    The result vector will have the same selection vector and count as the input
 *                    vector.
 */
template <typename InputType, typename ResultType, typename Op>
inline void UnaryOperation_HandleNull(const Vector &input, Vector *result) {
  auto *input_data = reinterpret_cast<InputType *>(input.GetData());
  auto *result_data = reinterpret_cast<ResultType *>(result->GetData());

  result->Resize(input.GetSize());
  result->GetMutableNullMask()->Reset();
  result->SetFilteredTupleIdList(input.GetFilteredTupleIdList(), input.GetCount());

  if (input.GetNullMask().Any()) {
    VectorOps::Exec(input, [&](uint64_t i, uint64_t k) {
      result_data[i] = Op::Apply(input_data[i], input.GetNullMask()[i]);
    });
  } else {
    VectorOps::Exec(
        input, [&](uint64_t i, uint64_t k) { result_data[i] = Op::Apply(input_data[i], false); });
  }
}

/**
 * Helper function to execute a unary function on all active elements in an input vector and store
 * the results into an output vector. The function is evaluated on all active elements, including
 * NULL elements.
 * @tparam InputType The native CPP type of the values in the input vector.
 * @tparam ResultType The native CPP type of the values in the result vector.
 * @tparam Op The unary operation to perform on each element in the input.
 * @param input The input vector to read values from.
 * @param result The output vector where the results of the unary operation are written into. The
 *               result vector will have the same selection vector and count as the input vector.
 */
template <typename InputType, typename ResultType, typename Op>
inline void UnaryOperation(const Vector &input, Vector *result) {
  auto *input_data = reinterpret_cast<InputType *>(input.GetData());
  auto *result_data = reinterpret_cast<ResultType *>(result->GetData());

  VectorOps::Exec(input,
                  [&](uint64_t i, uint64_t k) { result_data[i] = Op::Apply(input_data[i]); });

  result->GetMutableNullMask()->Copy(input.GetNullMask());
  result->SetFilteredTupleIdList(input.GetFilteredTupleIdList(), input.GetCount());
}

}  // namespace tpl::sql
