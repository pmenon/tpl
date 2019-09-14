#include "sql/vector_operations/vector_operators.h"

#include "common/exception.h"

namespace tpl::sql {

namespace {

template <typename T>
void TemplatedGenerateOperation(Vector *vector, T start, T increment) {
  auto *data = reinterpret_cast<T *>(vector->data());
  auto value = start;
  VectorOps::Exec(*vector, [&](uint64_t i, uint64_t k) {
    data[i] = value;
    value += increment;
  });
}

}  // namespace

void VectorOps::Generate(tpl::sql::Vector *vector, int64_t start, int64_t increment) {
  if (!IsTypeNumeric(vector->type_id())) {
    throw InvalidTypeException(vector->type_id(),
                               "sequence generation only allowed on numeric vectors");
  }
  switch (vector->type_id()) {
    case TypeId::TinyInt: {
      TemplatedGenerateOperation<int8_t>(vector, start, increment);
      break;
    }
    case TypeId::SmallInt: {
      TemplatedGenerateOperation<int16_t>(vector, start, increment);
      break;
    }
    case TypeId::Integer: {
      TemplatedGenerateOperation<int32_t>(vector, start, increment);
      break;
    }
    case TypeId::BigInt: {
      TemplatedGenerateOperation<int64_t>(vector, start, increment);
      break;
    }
    case TypeId::Hash: {
      TemplatedGenerateOperation<hash_t>(vector, start, increment);
      break;
    }
    case TypeId::Pointer: {
      TemplatedGenerateOperation<uintptr_t>(vector, start, increment);
      break;
    }
    case TypeId::Float: {
      TemplatedGenerateOperation<float>(vector, start, increment);
      break;
    }
    case TypeId::Double: {
      TemplatedGenerateOperation<double>(vector, start, increment);
      break;
    }
    default: {
      UNREACHABLE("Impossible type in switch. Should have been caught in if-guard earlier!");
    }
  }
}

}  // namespace tpl::sql
