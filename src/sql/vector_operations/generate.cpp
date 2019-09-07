#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

namespace {

template <typename T>
void GenerateSequenceImpl(Vector *vector, T start, T increment) {
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
    throw std::runtime_error("Sequence generation only allowed on numeric vectors");
  }
  switch (vector->type_id()) {
    case TypeId::TinyInt: {
      GenerateSequenceImpl<int8_t>(vector, start, increment);
      break;
    }
    case TypeId::SmallInt: {
      GenerateSequenceImpl<int16_t>(vector, start, increment);
      break;
    }
    case TypeId::Integer: {
      GenerateSequenceImpl<int32_t>(vector, start, increment);
      break;
    }
    case TypeId::BigInt: {
      GenerateSequenceImpl<int64_t>(vector, start, increment);
      break;
    }
    case TypeId::Hash: {
      GenerateSequenceImpl<hash_t>(vector, start, increment);
      break;
    }
    case TypeId::Pointer: {
      GenerateSequenceImpl<uintptr_t>(vector, start, increment);
      break;
    }
    case TypeId::Float: {
      GenerateSequenceImpl<float>(vector, start, increment);
      break;
    }
    case TypeId::Double: {
      GenerateSequenceImpl<double>(vector, start, increment);
      break;
    }
    default: {
      UNREACHABLE(
          "Impossible type in switch. Should have been caught in if-guard "
          "earlier!");
    }
  }
}

}  // namespace tpl::sql
