#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

namespace {

template <typename T>
void FillImpl(Vector *vector, T val) {
  auto *data = reinterpret_cast<T *>(vector->data());
  VectorOps::Exec(*vector, [&](u64 i, u64 k) { data[i] = val; });
}

}  // namespace

void VectorOps::Fill(Vector *vector, const GenericValue &value) {
  if (vector->count_ == 0) {
    return;
  }

  if (value.is_null()) {
    vector->null_mask_.set();
    return;
  }

  vector->null_mask_.reset();

  switch (vector->type_) {
    case TypeId::Boolean: {
      TPL_ASSERT(value.type_id() == TypeId::Boolean,
                 "Bool value not set in value!");
      FillImpl(vector, value.value_.boolean);
      break;
    }
    case TypeId::TinyInt: {
      TPL_ASSERT(value.type_id() == TypeId::TinyInt,
                 "Integer value not set in value!");
      FillImpl(vector, value.value_.tinyint);
      break;
    }
    case TypeId::SmallInt: {
      TPL_ASSERT(value.type_id() == TypeId::SmallInt,
                 "Integer value not set in value!");
      FillImpl(vector, value.value_.smallint);
      break;
    }
    case TypeId::Integer: {
      TPL_ASSERT(value.type_id() == TypeId::Integer,
                 "Integer value not set in value!");
      FillImpl(vector, value.value_.integer);
      break;
    }
    case TypeId::BigInt: {
      TPL_ASSERT(value.type_id() == TypeId::BigInt,
                 "Integer value not set in value!");
      FillImpl(vector, value.value_.bigint);
      break;
    }
    case TypeId::Float: {
      TPL_ASSERT(value.type_id() == TypeId::Float,
                 "Floating point value not set in value!");
      FillImpl(vector, value.value_.float_);
      break;
    }
    case TypeId::Double: {
      TPL_ASSERT(value.type_id() == TypeId::Double,
                 "Floating point value not set in value!");
      FillImpl(vector, value.value_.double_);
      break;
    }
    case TypeId::Varchar: {
      TPL_ASSERT(value.type_id() == TypeId::Varchar,
                 "String value not set in value!");
      auto *str = vector->strings_.AddString(value.str_value_);
      FillImpl(vector, str);
      break;
    }
    default: { UNREACHABLE("Impossible internal type"); }
  }
}

void VectorOps::FillNull(Vector *vector) { vector->null_mask_.set(); }

}  // namespace tpl::sql
