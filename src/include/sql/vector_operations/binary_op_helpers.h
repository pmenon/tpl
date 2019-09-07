#pragma once

namespace tpl::sql {

template <typename LeftType, typename RightType, typename ResultType, typename Op,
          bool IgnoreNull = false>
static inline void BinaryOperation_Constant_Vector(const Vector &left, const Vector &right,
                                                   Vector *result) {
  auto *left_data = reinterpret_cast<LeftType *>(left.data());
  auto *right_data = reinterpret_cast<RightType *>(right.data());
  auto *result_data = reinterpret_cast<ResultType *>(result->data());

  if (left.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    const auto &right_mask = right.null_mask();
    result->set_null_mask(right_mask);
    if (IgnoreNull && right_mask.Any()) {
      // Slow-path: need to check NULLs
      VectorOps::Exec(right.selection_vector(), right.count(), [&](uint64_t i, uint64_t k) {
        if (!right_mask[i]) {
          result_data[i] = Op::Apply(left_data[0], right_data[i]);
        }
      });
    } else {
      // Fast-path: no NULL checks
      VectorOps::Exec(right.selection_vector(), right.count(), [&](uint64_t i, uint64_t k) {
        result_data[i] = Op::Apply(left_data[0], right_data[i]);
      });
    }
  }

  result->SetSelectionVector(right.selection_vector(), right.count());
}

template <typename LeftType, typename RightType, typename ResultType, typename Op,
          bool IgnoreNull = false>
static inline void BinaryOperation_Vector_Constant(const Vector &left, const Vector &right,
                                                   Vector *result) {
  auto *left_data = reinterpret_cast<LeftType *>(left.data());
  auto *right_data = reinterpret_cast<RightType *>(right.data());
  auto *result_data = reinterpret_cast<ResultType *>(result->data());

  if (right.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    const auto &left_mask = left.null_mask();
    result->set_null_mask(left_mask);
    if (IgnoreNull && left_mask.Any()) {
      // Slow-path: need to check NULLs
      VectorOps::Exec(left.selection_vector(), left.count(), [&](uint64_t i, uint64_t k) {
        if (!left_mask[i]) {
          result_data[i] = Op::Apply(left_data[i], right_data[0]);
        }
      });
    } else {
      // Fast-path: no NULL checks
      VectorOps::Exec(left.selection_vector(), left.count(), [&](uint64_t i, uint64_t k) {
        result_data[i] = Op::Apply(left_data[i], right_data[0]);
      });
    }
  }

  result->SetSelectionVector(left.selection_vector(), left.count());
}

template <typename LeftType, typename RightType, typename ResultType, typename Op,
          bool IgnoreNull = false>
void BinaryOperation_Vector_Vector(const Vector &left, const Vector &right, Vector *result) {
  TPL_ASSERT(left.selection_vector() == right.selection_vector(),
             "Mismatched selection vectors for comparison");
  TPL_ASSERT(left.count() == right.count(), "Mismatched vector counts for comparison");

  auto *left_data = reinterpret_cast<LeftType *>(left.data());
  auto *right_data = reinterpret_cast<RightType *>(right.data());
  auto *result_data = reinterpret_cast<ResultType *>(result->data());

  const Vector::NullMask result_mask = left.null_mask() | right.null_mask();

  if (IgnoreNull && result_mask.Any()) {
    // Slow-path: need to check NULLs
    VectorOps::Exec(left.selection_vector(), left.count(), [&](uint64_t i, uint64_t k) {
      if (!result_mask[i]) {
        result_data[i] = Op::Apply(left_data[i], right_data[i]);
      }
    });
  } else {
    // Fast-path: no NULL checks
    VectorOps::Exec(left.selection_vector(), left.count(), [&](uint64_t i, uint64_t k) {
      result_data[i] = Op::Apply(left_data[i], right_data[i]);
    });
  }

  result->set_null_mask(result_mask);
  result->SetSelectionVector(left.selection_vector(), left.count());
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
static inline void BinaryOperation(const Vector &left, const Vector &right, Vector *result) {
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
