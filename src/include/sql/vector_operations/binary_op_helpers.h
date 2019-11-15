#pragma once

#include "common/exception.h"
#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

inline void CheckBinaryOperation(const Vector &left, const Vector &right, Vector *result) {
  if (left.GetTypeId() != right.GetTypeId()) {
    throw TypeMismatchException(left.GetTypeId(), right.GetTypeId(),
                                "left and right vector types to binary operation must be the same");
  }
  if (left.GetTypeId() != result->GetTypeId()) {
    throw TypeMismatchException(left.GetTypeId(), result->GetTypeId(),
                                "result type of binary operation must be the same as input types");
  }
  if (!left.IsConstant() && !right.IsConstant() && left.GetCount() != right.GetCount()) {
    throw Exception(ExceptionType::Cardinality,
                    "left and right input vectors to binary operation must have the same size");
  }
}

template <typename LeftType, typename RightType, typename ResultType, typename Op,
          bool IgnoreNull = false>
inline void BinaryOperation_Constant_Vector(const Vector &left, const Vector &right,
                                            Vector *result) {
  auto *left_data = reinterpret_cast<LeftType *>(left.GetData());
  auto *right_data = reinterpret_cast<RightType *>(right.GetData());
  auto *result_data = reinterpret_cast<ResultType *>(result->GetData());

  if (left.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    result->GetMutableNullMask()->Copy(right.GetNullMask());

    if (IgnoreNull && result->GetNullMask().Any()) {
      // Slow-path: need to check NULLs
      VectorOps::Exec(right.GetFilteredTupleIdList(), right.GetCount(),
                      [&](uint64_t i, uint64_t k) {
                        if (!result->GetNullMask()[i]) {
                          result_data[i] = Op::Apply(left_data[0], right_data[i]);
                        }
                      });
    } else {
      // Fast-path: no NULL checks
      VectorOps::Exec(
          right.GetFilteredTupleIdList(), right.GetCount(),
          [&](uint64_t i, uint64_t k) { result_data[i] = Op::Apply(left_data[0], right_data[i]); });
    }
  }

  result->SetFilteredTupleIdList(right.GetFilteredTupleIdList(), right.GetCount());
}

template <typename LeftType, typename RightType, typename ResultType, typename Op,
          bool IgnoreNull = false>
inline void BinaryOperation_Vector_Constant(const Vector &left, const Vector &right,
                                            Vector *result) {
  auto *left_data = reinterpret_cast<LeftType *>(left.GetData());
  auto *right_data = reinterpret_cast<RightType *>(right.GetData());
  auto *result_data = reinterpret_cast<ResultType *>(result->GetData());

  if (right.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    // Left input vector's NULL mask becomes result's NULL mask
    result->GetMutableNullMask()->Copy(left.GetNullMask());

    if (IgnoreNull && result->GetNullMask().Any()) {
      // Slow-path: need to check NULLs
      VectorOps::Exec(left.GetFilteredTupleIdList(), left.GetCount(), [&](uint64_t i, uint64_t k) {
        if (!result->GetNullMask()[i]) {
          result_data[i] = Op::Apply(left_data[i], right_data[0]);
        }
      });
    } else {
      // Fast-path: no NULL checks
      VectorOps::Exec(left.GetFilteredTupleIdList(), left.GetCount(), [&](uint64_t i, uint64_t k) {
        result_data[i] = Op::Apply(left_data[i], right_data[0]);
      });
    }
  }

  result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());
}

template <typename LeftType, typename RightType, typename ResultType, typename Op,
          bool IgnoreNull = false>
inline void BinaryOperation_Vector_Vector(const Vector &left, const Vector &right, Vector *result) {
  TPL_ASSERT(left.GetFilteredTupleIdList() == right.GetFilteredTupleIdList(),
             "Mismatched selection vectors for comparison");
  TPL_ASSERT(left.GetCount() == right.GetCount(), "Mismatched vector counts for comparison");

  auto *left_data = reinterpret_cast<LeftType *>(left.GetData());
  auto *right_data = reinterpret_cast<RightType *>(right.GetData());
  auto *result_data = reinterpret_cast<ResultType *>(result->GetData());

  result->GetMutableNullMask()->Copy(left.GetNullMask()).Union(right.GetNullMask());

  if (IgnoreNull && result->GetNullMask().Any()) {
    VectorOps::Exec(left.GetFilteredTupleIdList(), left.GetCount(), [&](uint64_t i, uint64_t k) {
      if (!result->GetNullMask()[i]) {
        result_data[i] = Op::Apply(left_data[i], right_data[i]);
      }
    });
  } else {
    VectorOps::Exec(left.GetFilteredTupleIdList(), left.GetCount(), [&](uint64_t i, uint64_t k) {
      result_data[i] = Op::Apply(left_data[i], right_data[i]);
    });
  }

  result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());
}

/**
 * Helper function to execute a binary operation on two input vectors and store the result into an
 * output vector. The operations are performed only on the active elements in the input vectors. It
 * is assumed that the input vectors have the same size and selection vectors (if any).
 *
 * After the function returns, the result vector will have the same selection vector and count as
 * the inputs.
 *
 * @tparam LeftType The native CPP type of the elements in the first input vector.
 * @tparam RightType The native CPP type of the elements in the second input vector.
 * @tparam ResultType The native CPP type of the elements in the result output vector.
 * @tparam Op The binary operation to perform. Each invocation will receive an element from the
 *            first and second input vectors and must produce an element that is stored in the
 *            result vector.
 * @tparam IgnoreNull Flag indicating if the operation should skip NULL values as they occur in
 *                    either inputs.
 * @param left The left input.
 * @param right The right input.
 * @param[out] result The result vector.
 */
template <typename LeftType, typename RightType, typename ResultType, typename Op,
          bool IgnoreNull = false>
inline void BinaryOperation(const Vector &left, const Vector &right, Vector *result) {
  if (left.IsConstant()) {
    BinaryOperation_Constant_Vector<LeftType, RightType, ResultType, Op, IgnoreNull>(left, right,
                                                                                     result);
  } else if (right.IsConstant()) {
    BinaryOperation_Vector_Constant<LeftType, RightType, ResultType, Op, IgnoreNull>(left, right,
                                                                                     result);
  } else {
    BinaryOperation_Vector_Vector<LeftType, RightType, ResultType, Op, IgnoreNull>(left, right,
                                                                                   result);
  }
}

}  // namespace tpl::sql
