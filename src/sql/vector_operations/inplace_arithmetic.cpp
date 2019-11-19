#include "sql/vector_operations/vector_operators.h"

#include "common/settings.h"
#include "sql/operations/numeric_inplace_operators.h"
#include "sql/vector_operations/numeric_inplace_op_helpers.h"

namespace tpl::sql {

void VectorOps::AddInPlace(Vector *left, const Vector &right) {
  // Sanity check
  CheckInplaceOperation(left, right);

  // Lift-off
  switch (left->GetTypeId()) {
    case TypeId::TinyInt:
      TemplatedInPlaceOperation<int8_t, int8_t, tpl::sql::AddInPlace>(left, right);
      break;
    case TypeId::SmallInt:
      TemplatedInPlaceOperation<int16_t, int16_t, tpl::sql::AddInPlace>(left, right);
      break;
    case TypeId::Integer:
      TemplatedInPlaceOperation<int32_t, int32_t, tpl::sql::AddInPlace>(left, right);
      break;
    case TypeId::BigInt:
      TemplatedInPlaceOperation<int64_t, int64_t, tpl::sql::AddInPlace>(left, right);
      break;
    case TypeId::Float:
      TemplatedInPlaceOperation<float, float, tpl::sql::AddInPlace>(left, right);
      break;
    case TypeId::Double:
      TemplatedInPlaceOperation<double, double, tpl::sql::AddInPlace>(left, right);
      break;
    case TypeId::Pointer:
      TemplatedInPlaceOperation<uint64_t, uint64_t, tpl::sql::AddInPlace>(left, right);
      break;
    default:
      throw InvalidTypeException(left->GetTypeId(),
                                 "invalid type for in-place arithmetic operation");
  }
}

}  // namespace tpl::sql
