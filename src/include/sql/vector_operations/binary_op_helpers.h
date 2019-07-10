#pragma once

namespace tpl::sql {

template <typename LeftType, typename RightType, typename ResultType,
          typename Op, bool IgnoreNull = false>
static inline void BinaryOperation_Constant_Vector(const Vector &left,
                                                   const Vector &right,
                                                   Vector *result) {
  auto *RESTRICT left_data = reinterpret_cast<LeftType *>(left.data());
  auto *RESTRICT right_data = reinterpret_cast<RightType *>(right.data());
  auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->data());

  if (left.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    const auto &right_mask = right.null_mask();
    result->set_null_mask(right_mask);
    if (IgnoreNull || right_mask.any()) {
      // Slow-path: need to check NULLs
      VectorOps::Exec(
          right.selection_vector(), right.count(), [&](u64 i, u64 k) {
            if (!right_mask[i]) {
              result_data[i] = Op::Apply(left_data[0], right_data[i]);
            }
          });
    } else {
      // Fast-path: no NULL checks
      VectorOps::Exec(right.selection_vector(), right.count(),
                      [&](u64 i, u64 k) {
                        result_data[i] = Op::Apply(left_data[0], right_data[i]);
                      });
    }
  }

  result->SetSelectionVector(right.selection_vector(), right.count());
}

template <typename LeftType, typename RightType, typename ResultType,
          typename Op, bool IgnoreNull = false>
static inline void BinaryOperation_Vector_Constant(const Vector &left,
                                                   const Vector &right,
                                                   Vector *result) {
  auto *RESTRICT left_data = reinterpret_cast<LeftType *>(left.data());
  auto *RESTRICT right_data = reinterpret_cast<RightType *>(right.data());
  auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->data());

  if (right.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    const auto &left_mask = left.null_mask();
    result->set_null_mask(left_mask);
    if (IgnoreNull || left_mask.any()) {
      // Slow-path: need to check NULLs
      VectorOps::Exec(left.selection_vector(), left.count(), [&](u64 i, u64 k) {
        if (!left_mask[i]) {
          result_data[i] = Op::Apply(left_data[i], right_data[0]);
        }
      });
    } else {
      // Fast-path: no NULL checks
      VectorOps::Exec(left.selection_vector(), left.count(), [&](u64 i, u64 k) {
        result_data[i] = Op::Apply(left_data[i], right_data[0]);
      });
    }
  }

  result->SetSelectionVector(left.selection_vector(), left.count());
}

template <typename LeftType, typename RightType, typename ResultType,
          typename Op, bool IgnoreNull = false>
void BinaryOperation_Vector_Vector(const Vector &left, const Vector &right,
                                   Vector *result) {
  TPL_ASSERT(left.selection_vector() == right.selection_vector(),
             "Mismatched selection vectors for comparison");
  TPL_ASSERT(left.count() == right.count(),
             "Mismatched vector counts for comparison");

  auto *RESTRICT left_data = reinterpret_cast<LeftType *>(left.data());
  auto *RESTRICT right_data = reinterpret_cast<RightType *>(right.data());
  auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->data());

  result->set_null_mask(left.null_mask() | right.null_mask());

  if (IgnoreNull || result->null_mask().any()) {
    // Slow-path: need to check NULLs
    VectorOps::Exec(left.selection_vector(), left.count(), [&](u64 i, u64 k) {
      if (!result->null_mask()[i]) {
        result_data[i] = Op::Apply(left_data[i], right_data[i]);
      }
    });
  } else {
    // Fast-path: no NULL checks
    VectorOps::Exec(left.selection_vector(), left.count(), [&](u64 i, u64 k) {
      result_data[i] = Op::Apply(left_data[i], right_data[i]);
    });
  }

  result->SetSelectionVector(left.selection_vector(), left.count());
}

template <typename LeftType, typename RightType, typename ResultType,
          typename Op, bool IgnoreNull = false>
static inline void BinaryOperation(const Vector &left, const Vector &right,
                                   Vector *result) {
  if (left.IsConstant()) {
    BinaryOperation_Constant_Vector<LeftType, RightType, ResultType, Op,
                                    IgnoreNull>(left, right, result);
  } else if (right.IsConstant()) {
    BinaryOperation_Vector_Constant<LeftType, RightType, ResultType, Op,
                                    IgnoreNull>(left, right, result);
  } else {
    BinaryOperation_Vector_Vector<LeftType, RightType, ResultType, Op,
                                  IgnoreNull>(left, right, result);
  }
}

}  // namespace tpl::sql
