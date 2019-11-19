#pragma once

#include "common/exception.h"
#include "sql/vector_operations/traits.h"
#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

/**
 *
 * @param left
 * @param right
 * @param result
 */
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
inline void TemplatedBinaryOperation_Constant_Vector(const Vector &left, const Vector &right,
                                                     Vector *result) {
  auto *left_data = reinterpret_cast<LeftType *>(left.GetData());
  auto *right_data = reinterpret_cast<RightType *>(right.GetData());
  auto *result_data = reinterpret_cast<ResultType *>(result->GetData());

  result->Resize(right.GetSize());
  result->SetFilteredTupleIdList(right.GetFilteredTupleIdList(), right.GetCount());

  if (left.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    result->GetMutableNullMask()->Copy(right.GetNullMask());

    if (IgnoreNull && result->GetNullMask().Any()) {
      // Slow-path: need to check NULLs
      VectorOps::Exec(right, [&](uint64_t i, uint64_t k) {
        if (!result->GetNullMask()[i]) {
          result_data[i] = Op::Apply(left_data[0], right_data[i]);
        }
      });
    } else {
      // Fast-path: no NULL checks

      // If it's faster to perform a full computation given the selectivity of
      // the vector and the operation we're performing, do so. Otherwise, use a
      // regular vector iteration.

      if (traits::ShouldPerformFullCompute<RightType, Op>()(right.GetFilteredTupleIdList())) {
        VectorOps::ExecIgnoreFilter(right, [&](uint64_t i, uint64_t k) {
          result_data[i] = Op::Apply(left_data[0], right_data[i]);
        });
      } else {
        VectorOps::Exec(right, [&](uint64_t i, uint64_t k) {
          result_data[i] = Op::Apply(left_data[0], right_data[i]);
        });
      }
    }
  }
}

template <typename LeftType, typename RightType, typename ResultType, typename Op,
          bool IgnoreNull = false>
inline void TemplatedBinaryOperation_Vector_Constant(const Vector &left, const Vector &right,
                                                     Vector *result) {
  auto *left_data = reinterpret_cast<LeftType *>(left.GetData());
  auto *right_data = reinterpret_cast<RightType *>(right.GetData());
  auto *result_data = reinterpret_cast<ResultType *>(result->GetData());

  result->Resize(left.GetSize());
  result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());

  if (right.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    // Left input vector's NULL mask becomes result's NULL mask
    result->GetMutableNullMask()->Copy(left.GetNullMask());

    if (IgnoreNull && result->GetNullMask().Any()) {
      // Slow-path: need to check NULLs
      VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
        if (!result->GetNullMask()[i]) {
          result_data[i] = Op::Apply(left_data[i], right_data[0]);
        }
      });
    } else {
      // Fast-path: no NULL checks

      // If it's faster to perform a full computation given the selectivity of
      // the vector and the operation we're performing, do so. Otherwise, use a
      // regular vector iteration.

      if (traits::ShouldPerformFullCompute<LeftType, Op>()(left.GetFilteredTupleIdList())) {
        VectorOps::ExecIgnoreFilter(left, [&](uint64_t i, uint64_t k) {
          result_data[i] = Op::Apply(left_data[0], right_data[i]);
        });
      } else {
        VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
          result_data[i] = Op::Apply(left_data[i], right_data[0]);
        });
      }
    }
  }

  result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());
}

template <typename LeftType, typename RightType, typename ResultType, typename Op,
          bool IgnoreNull = false>
inline void TemplatedBinaryOperation_Vector_Vector(const Vector &left, const Vector &right,
                                                   Vector *result) {
  TPL_ASSERT(left.GetFilteredTupleIdList() == right.GetFilteredTupleIdList(),
             "Mismatched selection vectors for comparison");
  TPL_ASSERT(left.GetCount() == right.GetCount(), "Mismatched vector counts for comparison");

  auto *left_data = reinterpret_cast<LeftType *>(left.GetData());
  auto *right_data = reinterpret_cast<RightType *>(right.GetData());
  auto *result_data = reinterpret_cast<ResultType *>(result->GetData());

  result->Resize(left.GetSize());
  result->GetMutableNullMask()->Copy(left.GetNullMask()).Union(right.GetNullMask());
  result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());

  if (IgnoreNull && result->GetNullMask().Any()) {
    VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
      if (!result->GetNullMask()[i]) {
        result_data[i] = Op::Apply(left_data[i], right_data[i]);
      }
    });
  } else {
    // If it's faster to perform a full computation given the selectivity of
    // the vector and the operation we're performing, do so. Otherwise, use a
    // regular vector iteration.

    if (traits::ShouldPerformFullCompute<LeftType, Op>()(left.GetFilteredTupleIdList())) {
      VectorOps::ExecIgnoreFilter(left, [&](uint64_t i, uint64_t k) {
        result_data[i] = Op::Apply(left_data[i], right_data[i]);
      });
    } else {
      VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
        result_data[i] = Op::Apply(left_data[i], right_data[i]);
      });
    }
  }
}

/**
 * Helper function to execute a binary operation on two input vectors and store the result into an
 * output vector. The operations are performed only on the active elements in the input vectors. It
 * is assumed that the input vectors have the same size and selection vectors (if any).
 *
 * This function leverages the tpl::sql::traits::ShouldPerformFullCompute trait to determine whether
 * the operation should be performed on ALL vector elements or just the active elements. Users can
 * control this feature by optionally specialization the trait for their operation type.
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
inline void TemplatedBinaryOperation(const Vector &left, const Vector &right, Vector *result) {
  if (left.IsConstant()) {
    TemplatedBinaryOperation_Constant_Vector<LeftType, RightType, ResultType, Op, IgnoreNull>(
        left, right, result);
  } else if (right.IsConstant()) {
    TemplatedBinaryOperation_Vector_Constant<LeftType, RightType, ResultType, Op, IgnoreNull>(
        left, right, result);
  } else {
    TemplatedBinaryOperation_Vector_Vector<LeftType, RightType, ResultType, Op, IgnoreNull>(
        left, right, result);
  }
}

}  // namespace tpl::sql
