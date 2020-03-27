#include "sql/vector_operations/vector_operations.h"

#include "common/settings.h"
#include "sql/operators/numeric_inplace_operators.h"
#include "sql/vector_operations/inplace_operation_executor.h"

namespace tpl::sql {

namespace traits {

template <typename T>
struct ShouldPerformFullCompute<tpl::sql::BitwiseANDInPlace<T>> {
  bool operator()(const TupleIdList *tid_list) const {
    auto full_compute_threshold =
        Settings::Instance()->GetDouble(Settings::Name::ArithmeticFullComputeOptThreshold);
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};

}  // namespace traits

namespace {

template <typename T, template <typename> typename Op>
void BitwiseOperation(Vector *left, const Vector &right) {
  InPlaceOperationExecutor::Execute<T, T, Op<T>>(left, right, Op<T>{});
}

}  // namespace

void VectorOps::BitwiseAndInPlace(Vector *left, const Vector &right) {
  // Sanity check
  CheckInplaceOperation(left, right);

  // Lift-off
  switch (left->GetTypeId()) {
    case TypeId::TinyInt:
      BitwiseOperation<int8_t, tpl::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::SmallInt:
      BitwiseOperation<int16_t, tpl::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::Integer:
      BitwiseOperation<int32_t, tpl::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::BigInt:
      BitwiseOperation<int64_t, tpl::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::Pointer:
      BitwiseOperation<uintptr_t, tpl::sql::BitwiseANDInPlace>(left, right);
      break;
    default:
      throw InvalidTypeException(left->GetTypeId(), "invalid type for in-place bitwise operation");
  }
}

}  // namespace tpl::sql
