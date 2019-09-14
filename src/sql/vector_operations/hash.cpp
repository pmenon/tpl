#include "sql/vector_operations/vector_operators.h"

#include "common/exception.h"
#include "sql/operations/hash_operators.h"
#include "sql/vector_operations/unary_op_helpers.h"

namespace tpl::sql {

namespace {

void CheckHashArguments(const Vector &input, Vector *result) {
  if (result->type_id() != TypeId::Hash) {
    throw InvalidTypeException(result->type_id(), "Output of Hash() operation must be hash");
  }
}

}  // namespace

void VectorOps::Hash(const Vector &input, Vector *result) {
  // Sanity check
  CheckHashArguments(input, result);

  // Lift-off
  switch (input.type_id()) {
    case TypeId::Boolean:
      UnaryOperation_HandleNull<bool, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::TinyInt:
      UnaryOperation_HandleNull<int8_t, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::SmallInt:
      UnaryOperation_HandleNull<int16_t, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Integer:
      UnaryOperation_HandleNull<int32_t, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::BigInt:
      UnaryOperation_HandleNull<int64_t, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Pointer:
      UnaryOperation_HandleNull<uintptr_t, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Float:
      UnaryOperation_HandleNull<float, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Double:
      UnaryOperation_HandleNull<double, hash_t, tpl::sql::Hash>(input, result);
      break;
    case TypeId::Varchar:
      UnaryOperation_HandleNull<const char *, hash_t, tpl::sql::Hash>(input, result);
      break;
    default:
      throw NotImplementedException("hashing not supported for vectors of type '{}'",
                                    TypeIdToString(input.type_id()));
  }

  result->SetSelectionVector(input.selection_vector(), input.count());
}

}  // namespace tpl::sql
