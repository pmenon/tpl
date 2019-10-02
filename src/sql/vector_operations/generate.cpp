#include "sql/vector_operations/vector_operators.h"

#include "common/exception.h"

namespace tpl::sql {

namespace {

void CheckGenerateArguments(const Vector &input) {
  if (!IsTypeNumeric(input.GetTypeId())) {
    throw InvalidTypeException(input.GetTypeId(),
                               "sequence generation only allowed on numeric vectors");
  }
}

template <typename T>
void TemplatedGenerateOperation(Vector *vector, T start, T increment) {
  auto *data = reinterpret_cast<T *>(vector->GetData());
  auto value = start;
  VectorOps::Exec(*vector, [&](uint64_t i, uint64_t k) {
    data[i] = value;
    value += increment;
  });
}

}  // namespace

void VectorOps::Generate(Vector *vector, int64_t start, int64_t increment) {
  // Sanity check
  CheckGenerateArguments(*vector);

  // Lift-off
  switch (vector->GetTypeId()) {
    case TypeId::TinyInt:
      TemplatedGenerateOperation<int8_t>(vector, start, increment);
      break;
    case TypeId::SmallInt:
      TemplatedGenerateOperation<int16_t>(vector, start, increment);
      break;
    case TypeId::Integer:
      TemplatedGenerateOperation<int32_t>(vector, start, increment);
      break;
    case TypeId::BigInt:
      TemplatedGenerateOperation<int64_t>(vector, start, increment);
      break;
    case TypeId::Hash:
      TemplatedGenerateOperation<hash_t>(vector, start, increment);
      break;
    case TypeId::Pointer:
      TemplatedGenerateOperation<uintptr_t>(vector, start, increment);
      break;
    case TypeId::Float:
      TemplatedGenerateOperation<float>(vector, start, increment);
      break;
    case TypeId::Double:
      TemplatedGenerateOperation<double>(vector, start, increment);
      break;
    default:
      throw NotImplementedException("Cannot generate into vector type {}",
                                    TypeIdToString(vector->GetTypeId()));
  }
}

}  // namespace tpl::sql
