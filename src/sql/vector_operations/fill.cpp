#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

namespace {

template <typename T>
void FillImpl(T *RESTRICT data, T val, u64 count, u32 *RESTRICT sel_vector) {
  VectorOps::Exec(sel_vector, count, [&](u64 i, u64 k) { data[i] = val; });
}

template <typename T>
void FillImpl(Vector *vector, T val) {
  auto *data = reinterpret_cast<T *>(vector->data());
  FillImpl(data, val, vector->count(), vector->selection_vector());
}

}  // namespace

void VectorOps::Fill(
    Vector *vector,
    const std::variant<bool, i64, f64, std::string_view> &value) {
  if (vector->count_ == 0) {
    return;
  }

  vector->null_mask_.reset();

  switch (vector->type_) {
    case TypeId::Boolean: {
      TPL_ASSERT(value.index() == 0, "Bool value not set in value!");
      FillImpl(vector, std::get<0>(value));
      break;
    }
    case TypeId::TinyInt: {
      TPL_ASSERT(value.index() == 1, "Integer value not set in value!");
      FillImpl(vector, static_cast<i8>(std::get<1>(value)));
      break;
    }
    case TypeId::SmallInt: {
      TPL_ASSERT(value.index() == 1, "Integer value not set in value!");
      FillImpl(vector, static_cast<i16>(std::get<1>(value)));
      break;
    }
    case TypeId::Integer: {
      TPL_ASSERT(value.index() == 1, "Integer value not set in value!");
      FillImpl(vector, static_cast<i32>(std::get<1>(value)));
      break;
    }
    case TypeId::BigInt: {
      TPL_ASSERT(value.index() == 1, "Integer value not set in value!");
      FillImpl(vector, std::get<1>(value));
      break;
    }
    case TypeId::Float: {
      TPL_ASSERT(value.index() == 2, "Floating point value not set in value!");
      FillImpl(vector, static_cast<f32>(std::get<2>(value)));
      break;
    }
    case TypeId::Double: {
      TPL_ASSERT(value.index() == 2, "Floating point value not set in value!");
      FillImpl(vector, std::get<2>(value));
      break;
    }
    case TypeId::Varchar: {
      TPL_ASSERT(value.index() == 3, "String value not set in value!");
      auto *str = vector->strings_.AddString(std::get<3>(value));
      FillImpl(vector, str);
      break;
    }
    default: { UNREACHABLE("Impossible internal type"); }
  }
}

void VectorOps::FillNull(Vector *vector) { vector->null_mask_.set(); }

}  // namespace tpl::sql