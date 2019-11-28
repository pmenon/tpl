#include "sql/vector_operations/vector_operators.h"

#include "common/settings.h"
#include "sql/operations/numeric_inplace_operators.h"
#include "sql/vector_operations/numeric_inplace_op_helpers.h"

namespace tpl::sql {

namespace traits {

#if 0
template <typename T, typename Op>
struct ShouldPerformFullCompute<T, Op,
                                std::enable_if_t<std::is_same_v<Op, tpl::sql::BitwiseANDInPlace>>> {
  bool operator()(const TupleIdList *tid_list) {
    auto full_compute_threshold =
        Settings::Instance()->GetDouble(Settings::Name::ArithmeticFullComputeOptThreshold);
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};
#endif

}  // namespace traits

void VectorOps::BitwiseANDInPlace(Vector *left, const Vector &right) {
  // Sanity check
  CheckInplaceOperation(left, right);

  // Lift-off
  switch (left->GetTypeId()) {
    case TypeId::TinyInt:
      TemplatedInPlaceOperation<int8_t, int8_t, tpl::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::SmallInt:
      TemplatedInPlaceOperation<int16_t, int16_t, tpl::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::Integer:
      TemplatedInPlaceOperation<int32_t, int32_t, tpl::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::BigInt:
      TemplatedInPlaceOperation<int64_t, int64_t, tpl::sql::BitwiseANDInPlace>(left, right);
      break;
    case TypeId::Pointer:
      TemplatedInPlaceOperation<uintptr_t, uintptr_t, tpl::sql::BitwiseANDInPlace>(left, right);
      break;
    default:
      throw InvalidTypeException(left->GetTypeId(), "invalid type for in-place bitwise operation");
  }
}

}  // namespace tpl::sql
