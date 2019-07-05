#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

namespace {

template <typename T>
void GenerateSequenceImpl(tpl::sql::Vector *vector, T start, T increment) {
  auto *data = reinterpret_cast<T *>(vector->data());
  T value = start;
  VectorOps::Exec(vector->selection_vector(), vector->count(),
                  [&](u64 i, u64 k) {
                    data[i] = value;
                    value += increment;
                  });
}

}  // namespace

void VectorOps::Generate(tpl::sql::Vector *vector, i64 start, i64 increment) {
  if (!IsTypeNumeric(vector->type_id())) {
    throw std::runtime_error(
        "Sequence generation only allowed on numeric vectors");
  }
  switch (vector->type_id()) {
    case TypeId::Boolean:
    case TypeId::TinyInt: {
      GenerateSequenceImpl<i8>(vector, start, increment);
      break;
    }
    case TypeId::SmallInt: {
      GenerateSequenceImpl<i16>(vector, start, increment);
      break;
    }
    case TypeId::Integer: {
      GenerateSequenceImpl<i32>(vector, start, increment);
      break;
    }
    case TypeId::BigInt: {
      GenerateSequenceImpl<i64>(vector, start, increment);
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
      GenerateSequenceImpl<f32>(vector, start, increment);
      break;
    }
    case TypeId::Double: {
      GenerateSequenceImpl<f64>(vector, start, increment);
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