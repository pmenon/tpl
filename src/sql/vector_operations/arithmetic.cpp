#include "sql/vector_operations/vector_operators.h"

#include "common/settings.h"
#include "sql/operations/numeric_binary_operators.h"
#include "sql/vector_operations/binary_op_helpers.h"

namespace tpl::sql {

namespace traits {

// Specialized struct to enable full-computation.
template <typename T, typename Op>
struct ShouldPerformFullCompute<
    T, Op,
    std::enable_if_t<std::is_same_v<Op, tpl::sql::Add> || std::is_same_v<Op, tpl::sql::Subtract> ||
                     std::is_same_v<Op, tpl::sql::Multiply>>> {
  bool operator()(const TupleIdList *tid_list) {
    auto full_compute_threshold =
        Settings::Instance()->GetDouble(Settings::Name::ArithmeticFullComputeOptThreshold);
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};

}  // namespace traits

namespace {

// TODO(pmenon): Instead of doing a branching zero-check, use a TID list to
//               quickly select which are non-zero, then iterate over that sub list?
// TODO(pmenon): Overflow?

template <typename LeftType, typename RightType, typename ResultType, typename Op>
void TemplatedDivModOperation_Constant_Vector(const Vector &left, const Vector &right,
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

    VectorOps::Exec(right, [&](uint64_t i, uint64_t k) {
      if (right_data[i] == RightType(0)) {
        result->GetMutableNullMask()->Set(i);
      } else {
        result_data[i] = Op::Apply(left_data[0], right_data[i]);
      }
    });
  }
}

template <typename LeftType, typename RightType, typename ResultType, typename Op>
void TemplatedDivModOperation_Vector_Constant(const Vector &left, const Vector &right,
                                              Vector *result) {
  auto *left_data = reinterpret_cast<LeftType *>(left.GetData());
  auto *right_data = reinterpret_cast<RightType *>(right.GetData());
  auto *result_data = reinterpret_cast<ResultType *>(result->GetData());

  result->Resize(left.GetSize());
  result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());

  if (right.IsNull(0)) {
    VectorOps::FillNull(result);
  } else {
    result->GetMutableNullMask()->Copy(left.GetNullMask());

    VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
      if (left_data[i] == LeftType(0)) {
        result->GetMutableNullMask()->Set(i);
      } else {
        result_data[i] = Op::Apply(left_data[i], right_data[0]);
      }
    });
  }
}

template <typename LeftType, typename RightType, typename ResultType, typename Op>
void TemplatedDivModOperation_Vector_Vector(const Vector &left, const Vector &right,
                                            Vector *result) {
  auto *left_data = reinterpret_cast<LeftType *>(left.GetData());
  auto *right_data = reinterpret_cast<RightType *>(right.GetData());
  auto *result_data = reinterpret_cast<ResultType *>(result->GetData());

  result->Resize(left.GetSize());
  result->GetMutableNullMask()->Copy(left.GetNullMask()).Union(right.GetNullMask());
  result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());

  VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
    if (left_data[i] == LeftType(0) || right_data[i] == RightType(0)) {
      result->GetMutableNullMask()->Set(i);
    } else {
      result_data[i] = Op::Apply(left_data[i], right_data[i]);
    }
  });
}

template <typename LeftType, typename RightType, typename ResultType, typename Op>
void XTemplatedDivModOperation(const Vector &left, const Vector &right, Vector *result) {
  if (left.IsConstant()) {
    TemplatedDivModOperation_Constant_Vector<LeftType, RightType, ResultType, Op>(left, right,
                                                                                  result);
  } else if (right.IsConstant()) {
    TemplatedDivModOperation_Vector_Constant<LeftType, RightType, ResultType, Op>(left, right,
                                                                                  result);
  } else {
    TemplatedDivModOperation_Vector_Vector<LeftType, RightType, ResultType, Op>(left, right,
                                                                                result);
  }
}

// Helper function to execute a divide or modulo operations. The operations are
// performed only on the active elements in the input vectors.
template <typename Op>
void DivModOperation(const Vector &left, const Vector &right, Vector *result) {
  // Sanity check
  CheckBinaryOperation(left, right, result);

  // Lift-off
  switch (left.GetTypeId()) {
    case TypeId::TinyInt:
      XTemplatedDivModOperation<int8_t, int8_t, int8_t, Op>(left, right, result);
      break;
    case TypeId::SmallInt:
      XTemplatedDivModOperation<int16_t, int16_t, int16_t, Op>(left, right, result);
      break;
    case TypeId::Integer:
      XTemplatedDivModOperation<int32_t, int32_t, int32_t, Op>(left, right, result);
      break;
    case TypeId::BigInt:
      XTemplatedDivModOperation<int64_t, int64_t, int64_t, Op>(left, right, result);
      break;
    case TypeId::Float:
      XTemplatedDivModOperation<float, float, float, Op>(left, right, result);
      break;
    case TypeId::Double:
      XTemplatedDivModOperation<double, double, double, Op>(left, right, result);
      break;
    case TypeId::Pointer:
      XTemplatedDivModOperation<uint64_t, uint64_t, uint64_t, Op>(left, right, result);
      break;
    default:
      throw InvalidTypeException(left.GetTypeId(), "Invalid type for arithmetic operation");
  }
}

// Dispatch to the generic BinaryOperation() function with full types.
template <typename Op>
void BinaryArithmeticOperation(const Vector &left, const Vector &right, Vector *result) {
  // Sanity check
  CheckBinaryOperation(left, right, result);

  // Lift-off
  switch (left.GetTypeId()) {
    case TypeId::TinyInt:
      TemplatedBinaryOperation<int8_t, int8_t, int8_t, Op>(left, right, result);
      break;
    case TypeId::SmallInt:
      TemplatedBinaryOperation<int16_t, int16_t, int16_t, Op>(left, right, result);
      break;
    case TypeId::Integer:
      TemplatedBinaryOperation<int32_t, int32_t, int32_t, Op>(left, right, result);
      break;
    case TypeId::BigInt:
      TemplatedBinaryOperation<int64_t, int64_t, int64_t, Op>(left, right, result);
      break;
    case TypeId::Float:
      TemplatedBinaryOperation<float, float, float, Op>(left, right, result);
      break;
    case TypeId::Double:
      TemplatedBinaryOperation<double, double, double, Op>(left, right, result);
      break;
    case TypeId::Pointer:
      TemplatedBinaryOperation<uint64_t, uint64_t, uint64_t, Op>(left, right, result);
      break;
    default:
      throw InvalidTypeException(left.GetTypeId(), "Invalid type for arithmetic operation");
  }
}

}  // namespace

void VectorOps::Add(const Vector &left, const Vector &right, Vector *result) {
  BinaryArithmeticOperation<tpl::sql::Add>(left, right, result);
}

void VectorOps::Subtract(const Vector &left, const Vector &right, Vector *result) {
  BinaryArithmeticOperation<tpl::sql::Subtract>(left, right, result);
}

void VectorOps::Multiply(const Vector &left, const Vector &right, Vector *result) {
  BinaryArithmeticOperation<tpl::sql::Multiply>(left, right, result);
}

void VectorOps::Divide(const Vector &left, const Vector &right, Vector *result) {
  DivModOperation<tpl::sql::Divide>(left, right, result);
}

void VectorOps::Modulo(const Vector &left, const Vector &right, Vector *result) {
  DivModOperation<tpl::sql::Modulo>(left, right, result);
}

}  // namespace tpl::sql
