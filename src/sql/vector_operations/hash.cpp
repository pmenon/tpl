#include "sql/vector_operations/vector_operators.h"

#include "common/exception.h"
#include "sql/operations/hash_operators.h"
#include "sql/vector_operations/unary_op_helpers.h"

namespace tpl::sql {

namespace {

void CheckHashArguments(const Vector &input, Vector *result) {
  if (result->GetTypeId() != TypeId::Hash) {
    throw InvalidTypeException(result->GetTypeId(), "Output of Hash() operation must be hash");
  }
}

}  // namespace

void VectorOps::Hash(const Vector &input, Vector *result) {
  // Sanity check
  CheckHashArguments(input, result);

  // Lift-off
  switch (input.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedUnaryOperation_HandleNull<bool, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::TinyInt:
      TemplatedUnaryOperation_HandleNull<int8_t, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::SmallInt:
      TemplatedUnaryOperation_HandleNull<int16_t, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Integer:
      TemplatedUnaryOperation_HandleNull<int32_t, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::BigInt:
      TemplatedUnaryOperation_HandleNull<int64_t, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Pointer:
      TemplatedUnaryOperation_HandleNull<uintptr_t, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Float:
      TemplatedUnaryOperation_HandleNull<float, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Double:
      TemplatedUnaryOperation_HandleNull<double, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Date:
      TemplatedUnaryOperation_HandleNull<Date, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Varchar:
      TemplatedUnaryOperation_HandleNull<const VarlenEntry, hash_t, tpl::sql::Hash>(input, result);
      break;
    default:
      throw NotImplementedException("hashing not supported for vectors of type '{}'",
                                    TypeIdToString(input.GetTypeId()));
  }
}

}  // namespace tpl::sql
