#pragma once

#include <type_traits>
#include <utility>

#include "common/common.h"
#include "sql/vector.h"
#include "sql/vector_operations/traits.h"
#include "sql/vector_operations/vector_operations.h"

namespace tpl::sql {

class BinaryOperationExecutor : public AllStatic {
 public:
  /**
   * Execute a binary operation on all active elements contained in two input vectors, @em left and
   * @em right, and store the result into an output vector, @em result. An instance of the binary
   * operation templated type is created and used for the operation. Thus, it's assumed that it
   * contains no state.
   *
   * @pre 1. Both vectors cannot be constants.
   *      2. Both input vectors have the same type and shape.
   *      3. The template types of both inputs and the output match the underlying vector types.
   * @post The output vector has the same shape (size and filter status) as both inputs.
   *
   * @note This function leverages the tpl::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type. If you want to use this optimization, you cannot pass in a
   *       std::function; move your logic into a function object and pass an instance.
   *
   * @tparam LeftType The native CPP type of the elements in the first input vector.
   * @tparam RightType The native CPP type of the elements in the second input vector.
   * @tparam ResultType The native CPP type of the elements in the result output vector.
   * @tparam Op The binary operation to perform. Each invocation will receive an element from the
   *            first and second input vectors and must produce an element that is stored in the
   *            result vector.
   * @tparam IgnoreNull Flag indicating if the operation should skip NULL values as in either input.
   * @param left The left input.
   * @param right The right input.
   * @param[out] result The result vector.
   */
  template <typename LeftType, typename RightType, typename ResultType, typename Op,
            bool IgnoreNull = false>
  static void Execute(const Vector &left, const Vector &right, Vector *result) {
    Execute<LeftType, RightType, ResultType, Op, IgnoreNull>(left, right, result, Op{});
  }

  /**
   * Execute the provided binary operation, @em op, on all active elements contained in two input
   * vectors, @em left and @em right, and store the result into an output vector, @em result.
   *
   * @pre Both input vectors have the same type and shape. The template types of both inputs and the
   *      output match the underlying vector types.
   * @post The output vector has the same shape (size and filter status) as both inputs.
   *
   * @note This function leverages the tpl::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type. If you want to use this optimization, you cannot pass in a
   *       std::function; move your logic into a function object and pass an instance.
   *
   * @tparam LeftType The native CPP type of the elements in the first input vector.
   * @tparam RightType The native CPP type of the elements in the second input vector.
   * @tparam ResultType The native CPP type of the elements in the result output vector.
   * @tparam Op The binary operation to perform. Each invocation will receive an element from the
   *            first and second input vectors and must produce an element that is stored in the
   *            result vector.
   * @tparam IgnoreNull Flag indicating if the operation should skip NULL values as in either input.
   * @param left The left input.
   * @param right The right input.
   * @param[out] result The result vector.
   * @param op The binary operation.
   */
  template <typename LeftType, typename RightType, typename ResultType, typename Op,
            bool IgnoreNull = false>
  static void Execute(const Vector &left, const Vector &right, Vector *result, Op op) {
    // Ensure operator has correct interface.
    static_assert(std::is_invocable_r_v<ResultType, Op, LeftType, RightType>,
                  "Binary operation has invalid interface for given template arguments.");

    // Ensure at least one of the inputs are vectors.
    TPL_ASSERT(!left.IsConstant() || !right.IsConstant(),
               "Both inputs to binary cannot be constants");

    if (left.IsConstant()) {
      ExecuteImpl_Constant_Vector<LeftType, RightType, ResultType, Op, IgnoreNull>(left, right,
                                                                                   result, op);
    } else if (right.IsConstant()) {
      ExecuteImpl_Vector_Constant<LeftType, RightType, ResultType, Op, IgnoreNull>(left, right,
                                                                                   result, op);
    } else {
      ExecuteImpl_Vector_Vector<LeftType, RightType, ResultType, Op, IgnoreNull>(left, right,
                                                                                 result, op);
    }
  }

  /**
   * Evaluate the templated binary comparison function, @em Op, on elements in @em left and
   * @em right with TIDs in @em tid_list, retaining only those TIDs that for which the comparison
   * returns true.
   *
   * @pre 1. Both input vectors have the same type and shape.
   *      2. The template types of both inputs match the underlying vector types.
   *      3. The comparison operator must be callable as: bool(in1, in2)
   *      4. The TID list must not be larger than the largest input's vector capacity.
   *
   * @note This function leverages the tpl::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type.
   *
   * @tparam LeftType The native CPP type of the elements in the first input vector.
   * @tparam RightType The native CPP type of the elements in the second input vector.
   * @tparam Op The binary comparison operation to perform. Each invocation will receive an element
   *            from both input vectors and must produce a boolean output value, i.e., the result of
   *            the comparison.
   * @param left The left input.
   * @param right The right input.
   * @param[in,out] tid_list The list of TIDs to operate on and filter.
   */
  template <typename LeftType, typename RightType, typename Op>
  static void Select(const Vector &left, const Vector &right, TupleIdList *tid_list) {
    Select<LeftType, RightType, Op>(left, right, tid_list, Op{});
  }

  /**
   * Evaluate the provided binary comparison function, @em Op, on elements in @em left and
   * @em right with TIDs in @em tid_list, retaining only those TIDs that for which the comparison
   * returns true.
   *
   * @pre 1. Both input vectors have the same type and shape.
   *      2. The template types of both inputs match the underlying vector types.
   *      3. The comparison operator must be callable as: bool(in1, in2)
   *      4. The TID list must not be larger than the largest input's vector capacity.
   *
   * @note This function leverages the tpl::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type. If you want to use this optimization, you cannot pass in a
   *       std::function; move your logic into a function object and pass an instance.
   *
   * @tparam LeftType The native CPP type of the elements in the first input vector.
   * @tparam RightType The native CPP type of the elements in the second input vector.
   * @tparam Op The binary comparison operation to perform. Each invocation will receive an element
   *            from both input vectors and must produce a boolean output value, i.e., the result of
   *            the comparison.
   * @param left The left input.
   * @param right The right input.
   * @param[in,out] tid_list The list of TIDs to operate on and filter.
   * @param op The binary comparison operator.
   */
  template <typename LeftType, typename RightType, typename Op>
  static void Select(const Vector &left, const Vector &right, TupleIdList *tid_list, Op op) {
    static_assert(std::is_invocable_r_v<bool, Op, LeftType, RightType>,
                  "Binary selection has invalid interface for given template arguments.");

    // Ensure at least one of the inputs are vectors.
    TPL_ASSERT(!left.IsConstant() || !right.IsConstant(),
               "Both inputs to binary cannot be constants");

    if (left.IsConstant()) {
      SelectImpl_Constant_Vector<LeftType, RightType, Op>(left, right, tid_list, op);
    } else if (right.IsConstant()) {
      SelectImpl_Vector_Constant<LeftType, RightType, Op>(left, right, tid_list, op);
    } else {
      SelectImpl_Vector_Vector<LeftType, RightType, Op>(left, right, tid_list, op);
    }
  }

 private:
  // Binary operation where the left input is a constant value.
  template <typename LeftType, typename RightType, typename ResultType, typename Op,
            bool IgnoreNull>
  static void ExecuteImpl_Constant_Vector(const Vector &left, const Vector &right, Vector *result,
                                          Op op) {
    auto *RESTRICT left_data = reinterpret_cast<LeftType *>(left.GetData());
    auto *RESTRICT right_data = reinterpret_cast<RightType *>(right.GetData());
    auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->GetData());

    result->Resize(right.GetSize());
    result->SetFilteredTupleIdList(right.GetFilteredTupleIdList(), right.GetCount());

    if (left.IsNull(0)) {
      VectorOps::FillNull(result);
    } else {
      result->GetMutableNullMask()->Copy(right.GetNullMask());

      if (IgnoreNull && result->GetNullMask().Any()) {
        VectorOps::Exec(right, [&](uint64_t i, uint64_t k) {
          if (!result->GetNullMask()[i]) {
            result_data[i] = op(left_data[0], right_data[i]);
          }
        });
      } else {
        if (traits::ShouldPerformFullCompute<Op>()(right.GetFilteredTupleIdList())) {
          VectorOps::ExecIgnoreFilter(right, [&](uint64_t i, uint64_t k) {
            result_data[i] = op(left_data[0], right_data[i]);
          });
        } else {
          VectorOps::Exec(right, [&](uint64_t i, uint64_t k) {
            result_data[i] = op(left_data[0], right_data[i]);
          });
        }
      }
    }
  }

  // Binary operation where the right input is a constant value.
  template <typename LeftType, typename RightType, typename ResultType, typename Op,
            bool IgnoreNull>
  static void ExecuteImpl_Vector_Constant(const Vector &left, const Vector &right, Vector *result,
                                          Op op) {
    auto *RESTRICT left_data = reinterpret_cast<LeftType *>(left.GetData());
    auto *RESTRICT right_data = reinterpret_cast<RightType *>(right.GetData());
    auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->GetData());

    result->Resize(left.GetSize());
    result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());

    if (right.IsNull(0)) {
      VectorOps::FillNull(result);
    } else {
      result->GetMutableNullMask()->Copy(left.GetNullMask());

      if (IgnoreNull && result->GetNullMask().Any()) {
        VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
          if (!result->GetNullMask()[i]) {
            result_data[i] = op(left_data[i], right_data[0]);
          }
        });
      } else {
        if (traits::ShouldPerformFullCompute<Op>()(left.GetFilteredTupleIdList())) {
          VectorOps::ExecIgnoreFilter(left, [&](uint64_t i, uint64_t k) {
            result_data[i] = op(left_data[i], right_data[0]);
          });
        } else {
          VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
            result_data[i] = op(left_data[i], right_data[0]);
          });
        }
      }
    }
  }

  // Binary operation where both inputs are vectors.
  template <typename LeftType, typename RightType, typename ResultType, typename Op,
            bool IgnoreNull>
  static void ExecuteImpl_Vector_Vector(const Vector &left, const Vector &right, Vector *result,
                                        Op op) {
    TPL_ASSERT(left.GetFilteredTupleIdList() == right.GetFilteredTupleIdList(),
               "Mismatched selection vectors for comparison");
    TPL_ASSERT(left.GetCount() == right.GetCount(), "Mismatched vector counts for comparison");

    auto *RESTRICT left_data = reinterpret_cast<LeftType *>(left.GetData());
    auto *RESTRICT right_data = reinterpret_cast<RightType *>(right.GetData());
    auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->GetData());

    result->Resize(left.GetSize());
    result->GetMutableNullMask()->Copy(left.GetNullMask()).Union(right.GetNullMask());
    result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());

    if (IgnoreNull && result->GetNullMask().Any()) {
      VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
        if (!result->GetNullMask()[i]) {
          result_data[i] = op(left_data[i], right_data[i]);
        }
      });
    } else {
      if (traits::ShouldPerformFullCompute<Op>()(left.GetFilteredTupleIdList())) {
        VectorOps::ExecIgnoreFilter(left, [&](uint64_t i, uint64_t k) {
          result_data[i] = op(left_data[i], right_data[i]);
        });
      } else {
        VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
          result_data[i] = op(left_data[i], right_data[i]);
        });
      }
    }
  }

  // Binary selection where the left input is a constant value.
  template <typename LeftType, typename RightType, typename Op>
  static void SelectImpl_Constant_Vector(const Vector &left, const Vector &right,
                                         TupleIdList *tid_list, Op op) {
    // If the scalar constant is NULL, all comparisons are NULL.
    if (left.IsNull(0)) {
      tid_list->Clear();
      return;
    }

    auto &constant = *reinterpret_cast<const LeftType *>(left.GetData());
    auto *RESTRICT right_data = reinterpret_cast<const RightType *>(right.GetData());

    // Safe full-compute. Refer to comment at start of file for explanation.
    if (traits::ShouldPerformFullCompute<Op>()(tid_list)) {
      TupleIdList::BitVectorType *bit_vector = tid_list->GetMutableBits();
      bit_vector->UpdateFull([&](uint64_t i) { return op(constant, right_data[i]); });
      bit_vector->Difference(right.GetNullMask());
      return;
    }

    // Remove all NULL entries from right input. Left constant is guaranteed non-NULL by this point.
    tid_list->GetMutableBits()->Difference(right.GetNullMask());

    // Filter
    tid_list->Filter([&](uint64_t i) { return op(constant, right_data[i]); });
  }

  // Binary selection where the right input is a constant value.
  template <typename LeftType, typename RightType, typename Op>
  static void SelectImpl_Vector_Constant(const Vector &left, const Vector &right,
                                         TupleIdList *tid_list, Op op) {
    // If the scalar constant is NULL, all comparisons are NULL.
    if (right.IsNull(0)) {
      tid_list->Clear();
      return;
    }

    auto *RESTRICT left_data = reinterpret_cast<const LeftType *>(left.GetData());
    auto &constant = *reinterpret_cast<const RightType *>(right.GetData());

    // Safe full-compute. Refer to comment at start of file for explanation.
    if (traits::ShouldPerformFullCompute<Op>()(tid_list)) {
      TupleIdList::BitVectorType *bit_vector = tid_list->GetMutableBits();
      bit_vector->UpdateFull([&](uint64_t i) { return op(left_data[i], constant); });
      bit_vector->Difference(left.GetNullMask());
      return;
    }

    // Remove all NULL entries from left input. Right constant is guaranteed non-NULL by this point.
    tid_list->GetMutableBits()->Difference(left.GetNullMask());

    // Filter
    tid_list->Filter([&](uint64_t i) { return op(left_data[i], constant); });
  }

  // Binary selection where both inputs are vectors.
  template <typename LeftType, typename RightType, typename Op>
  static void SelectImpl_Vector_Vector(const Vector &left, const Vector &right,
                                       TupleIdList *tid_list, Op op) {
    auto *RESTRICT left_data = reinterpret_cast<const LeftType *>(left.GetData());
    auto *RESTRICT right_data = reinterpret_cast<const RightType *>(right.GetData());

    // Safe full-compute. Refer to comment at start of file for explanation.
    if (traits::ShouldPerformFullCompute<Op>()(left.GetFilteredTupleIdList())) {
      TupleIdList::BitVectorType *bit_vector = tid_list->GetMutableBits();
      bit_vector->UpdateFull([&](uint64_t i) { return op(left_data[i], right_data[i]); });
      bit_vector->Difference(left.GetNullMask()).Difference(right.GetNullMask());
      return;
    }

    // Remove all NULL entries in either vector
    tid_list->GetMutableBits()->Difference(left.GetNullMask()).Difference(right.GetNullMask());

    // Filter
    tid_list->Filter([&](uint64_t i) { return op(left_data[i], right_data[i]); });
  }
};

}  // namespace tpl::sql
