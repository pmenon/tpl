#pragma once

#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

/**
 * Helper function to execute a unary function on all active elements in an input vector and store
 * the results into an output vector.
 *
 * @pre The input vector is allowed to be either a constant or a full vector, filtered or not.
 * @post The result vector will have the same shape as the input.
 *
 * @tparam InputType The native CPP type of the values in the input vector.
 * @tparam ResultType The native CPP type of the values in the result vector.
 * @tparam Op The unary operation to perform on each element in the input.
 * @tparam IgnoreNull Flag indicating if the operation should skip NULL values in the input.
 * @param input The input vector to read values from.
 * @param[out] result The output vector where the results of the unary operation are written into.
 *                    The result vector will have the same set of NULLs, selection vector, and count
 *                    as the input vector.
 */
template <typename InputType, typename ResultType, typename Op, bool IgnoreNull = false>
inline void TemplatedUnaryOperation(const Vector &input, Vector *result) {
  auto *RESTRICT input_data = reinterpret_cast<InputType *>(input.GetData());
  auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->GetData());

  result->Resize(input.GetSize());
  result->GetMutableNullMask()->Copy(input.GetNullMask());
  result->SetFilteredTupleIdList(input.GetFilteredTupleIdList(), input.GetCount());

  if (input.IsConstant()) {
    if (!input.IsNull(0)) {
      result_data[0] = Op::template Apply<InputType, ResultType>(input_data[0]);
    }
  } else {
    if (IgnoreNull && input.GetNullMask().Any()) {
      const auto &null_mask = input.GetNullMask();
      VectorOps::Exec(input, [&](uint64_t i, uint64_t k) {
        if (!null_mask[i]) {
          result_data[i] = Op::template Apply<InputType, ResultType>(input_data[i]);
        }
      });
    } else {
      VectorOps::Exec(input, [&](uint64_t i, uint64_t k) {
        result_data[i] = Op::template Apply<InputType, ResultType>(input_data[i]);
      });
    }
  }
}

}  // namespace tpl::sql
