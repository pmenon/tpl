#pragma once

namespace tpl::sql {

template <typename LeftType, typename RightType, typename ResultType,
          typename Op>
static inline void TemplatedBinaryOperationLoop_Constant_Vector(
    LeftType left_data, RightType *RESTRICT right_data,
    ResultType *RESTRICT result_data, u64 count, u32 *RESTRICT sel_vector) {
  VectorOps::Exec(sel_vector, count, [&](u64 i, u64 k) {
    result_data[i] = Op::Apply(left_data, right_data[i]);
  });
}

template <typename LeftType, typename RightType, typename ResultType,
          typename Op>
static inline void TemplatedBinaryOperationLoop_Vector_Constant(
    LeftType *RESTRICT left_data, RightType right_data,
    ResultType *RESTRICT result_data, u64 count, u32 *RESTRICT sel_vector) {
  VectorOps::Exec(sel_vector, count, [&](u64 i, u64 k) {
    result_data[i] = Op::Apply(left_data[i], right_data);
  });
}

template <typename LeftType, typename RightType, typename ResultType,
          typename Op>
void TemplatedBinaryOperation_Vector_Vector(LeftType *RESTRICT left_data,
                                            RightType *RESTRICT right_data,
                                            ResultType *RESTRICT result_data,
                                            u64 count,
                                            u32 *RESTRICT sel_vector) {
  VectorOps::Exec(sel_vector, count, [&](u64 i, u64 k) {
    result_data[i] = Op::Apply(left_data[i], right_data[i]);
  });
}

template <typename LeftType, typename RightType, typename ResultType,
          typename Op, bool IgnoreNull = false>
static inline void TemplatedBinaryOperation(const Vector &left,
                                            const Vector &right,
                                            Vector *result) {
  auto *left_data = reinterpret_cast<LeftType *>(left.data());
  auto *right_data = reinterpret_cast<RightType *>(right.data());
  auto *result_data = reinterpret_cast<ResultType *>(result->data());

  // Is the left vector just a single constant value?
  if (left.IsConstant()) {
    if (left.IsNull(0)) {
      // The left constant value is NULL, set the result to all NULLs.
      VectorOps::FillNull(result);
    } else {
      // Left is a non-null constant. First, we set the NULL bitmap of the
      // result to be that of the right vector. Then decide whether to perform
      // comparison checking NULLs or not.
      const auto left_const_val = left_data[0];
      const auto &right_mask = right.null_mask();
      result->set_null_mask(right_mask);
      if (IgnoreNull || right_mask.any()) {
        VectorOps::Exec(
            right.selection_vector(), right.count(), [&](u64 i, u64 k) {
              if (!right_mask[i]) {
                result_data[i] = Op::Apply(left_const_val, right_data[i]);
              }
            });
      } else {
        TemplatedBinaryOperationLoop_Constant_Vector<LeftType, RightType,
                                                     ResultType, Op>(
            left_const_val, right_data, result_data, right.count(),
            right.selection_vector());
      }
    }
    result->SetSelectionVector(right.selection_vector(), right.count());
    return;
  }

  // Is the right vector just a single constant value?
  if (right.IsConstant()) {
    if (right.IsNull(0)) {
      // The right constant value is NULL, set the result to all NULLs.
      VectorOps::FillNull(result);
    } else {
      // Right is a non-null constant. First, we set the NULL bitmap of the
      // result to be that of the right vector. Then decide whether to perform
      // comparison checking NULLs or not.
      const auto right_const_value = right_data[0];
      const auto &left_mask = left.null_mask();
      result->set_null_mask(left_mask);
      if (IgnoreNull || left_mask.any()) {
        VectorOps::Exec(
            left.selection_vector(), left.count(), [&](u64 i, u64 k) {
              if (!left_mask[i]) {
                result_data[i] = Op::Apply(left_data[i], right_const_value);
              }
            });
      } else {
        TemplatedBinaryOperationLoop_Vector_Constant<LeftType, RightType,
                                                     ResultType, Op>(
            left_data, right_const_value, result_data, left.count(),
            left.selection_vector());
      }
    }
    result->SetSelectionVector(left.selection_vector(), left.count());
    return;
  }

  TPL_ASSERT(left.selection_vector() == right.selection_vector(),
             "Mismatched selection vectors for comparison");
  TPL_ASSERT(left.count() == right.count(),
             "Mismatched vector counts for comparison");

  result->set_null_mask(left.null_mask() | right.null_mask());

  if (IgnoreNull || left.null_mask().any() || right.null_mask().any()) {
    VectorOps::Exec(left.selection_vector(), left.count(), [&](u64 i, u64 k) {
      if (!result->null_mask()[i]) {
        result_data[i] = Op::Apply(left_data[i], right_data[i]);
      }
    });
  } else {
    TemplatedBinaryOperation_Vector_Vector<LeftType, RightType, ResultType, Op>(
        left_data, right_data, result_data, left.count(),
        left.selection_vector());
  }
}

}  // namespace tpl::sql
