#include "sql/vector.h"

#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "spdlog/fmt/fmt.h"

#include "sql/vector_operations/vector_operators.h"
#include "util/bit_util.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Vector
// ---------------------------------------------------------

Vector::Vector(TypeId type)
    : type_(type), count_(0), num_elems_(0), data_(nullptr), sel_vector_(nullptr) {
  // Since vector capacity can never exceed kDefaultVectorSize, we reserve upon creation to remove
  // allocations as the vector is resized.
  null_mask_.Reserve(kDefaultVectorSize);
  null_mask_.Resize(num_elems_);
}

Vector::Vector(TypeId type, bool create_data, bool clear)
    : type_(type), count_(0), num_elems_(0), data_(nullptr), sel_vector_(nullptr) {
  // Since vector capacity can never exceed kDefaultVectorSize, we reserve upon creation to remove
  // allocations as the vector is resized.
  null_mask_.Reserve(kDefaultVectorSize);
  null_mask_.Resize(num_elems_);
  if (create_data) {
    Initialize(type, clear);
  }
}

Vector::Vector(TypeId type, byte *data, uint64_t size)
    : type_(type), count_(size), num_elems_(size), data_(data), sel_vector_(nullptr) {
  TPL_ASSERT(data != nullptr, "Cannot create vector from NULL data pointer");
  TPL_ASSERT(size <= kDefaultVectorSize, "External data size too large");
  // Since vector capacity can never exceed kDefaultVectorSize, we reserve upon creation to remove
  // allocations as the vector is resized.
  null_mask_.Reserve(kDefaultVectorSize);
  null_mask_.Resize(num_elems_);
}

Vector::~Vector() { Destroy(); }

void Vector::Initialize(const TypeId new_type, const bool clear) {
  varlens_.Destroy();

  type_ = new_type;

  // Since the caller controls whether we zero the vector's memory upon creation, we need to be
  // careful about exactly how we allocate it. std::make_unique<T[]>(...) will always zero-out the
  // memory since it value-initializes the array. Similarly, new T[...]() also value-initializes
  // each element. We want to avoid both of these if the caller didn't explicitly request it. So, we
  // allocate the array using new T[...] (without the '()'), and clear the contents if requested.

  std::size_t num_bytes = kDefaultVectorSize * GetTypeIdSize(type_);
  owned_data_ = std::unique_ptr<byte[]>(new byte[num_bytes]);
  data_ = owned_data_.get();
  if (clear) std::memset(data_, 0, num_bytes);
}

void Vector::Destroy() {
  owned_data_.reset();
  varlens_.Destroy();
  data_ = nullptr;
  count_ = 0;
  num_elems_ = 0;
  sel_vector_ = nullptr;
  null_mask_.Reset();
}

GenericValue Vector::GetValue(const uint64_t index) const {
  TPL_ASSERT(index < count_, "Out-of-bounds vector access");
  if (IsNull(index)) {
    return GenericValue::CreateNull(type_);
  }
  auto actual_index = (sel_vector_ != nullptr ? sel_vector_[index] : index);
  switch (type_) {
    case TypeId::Boolean: {
      return GenericValue::CreateBoolean(reinterpret_cast<bool *>(data_)[actual_index]);
    }
    case TypeId::TinyInt: {
      return GenericValue::CreateTinyInt(reinterpret_cast<int8_t *>(data_)[actual_index]);
    }
    case TypeId::SmallInt: {
      return GenericValue::CreateSmallInt(reinterpret_cast<int16_t *>(data_)[actual_index]);
    }
    case TypeId::Integer: {
      return GenericValue::CreateInteger(reinterpret_cast<int32_t *>(data_)[actual_index]);
    }
    case TypeId::BigInt: {
      return GenericValue::CreateBigInt(reinterpret_cast<int64_t *>(data_)[actual_index]);
    }
    case TypeId::Hash: {
      return GenericValue::CreateHash(reinterpret_cast<hash_t *>(data_)[actual_index]);
    }
    case TypeId::Pointer: {
      return GenericValue::CreatePointer(reinterpret_cast<uintptr_t *>(data_)[actual_index]);
    }
    case TypeId::Float: {
      return GenericValue::CreateReal(reinterpret_cast<float *>(data_)[actual_index]);
    }
    case TypeId::Double: {
      return GenericValue::CreateDouble(reinterpret_cast<double *>(data_)[actual_index]);
    }
    case TypeId::Date: {
      return GenericValue::CreateDate(reinterpret_cast<Date *>(data_)[actual_index]);
    }
    case TypeId::Varchar: {
      const auto &varlen_str = reinterpret_cast<const VarlenEntry *>(data_)[actual_index];
      TPL_ASSERT(varlen_str.GetContent() != nullptr, "Null string in position not marked NULL!");
      return GenericValue::CreateVarchar(std::string_view(
          reinterpret_cast<const char *>(varlen_str.GetContent()), varlen_str.GetSize()));
    }
    default: {
      throw std::runtime_error(
          fmt::format("Cannot read value of type '{}' from vector", TypeIdToString(type_)));
    }
  }
}

void Vector::Resize(uint32_t size) {
  TPL_ASSERT(size <= kDefaultVectorSize, "Size too large");

  sel_vector_ = nullptr;
  count_ = size;
  num_elems_ = size;
  null_mask_.Resize(num_elems_);
}

void Vector::SetValue(const uint64_t index, const GenericValue &val) {
  TPL_ASSERT(index < count_, "Out-of-bounds vector access");
  TPL_ASSERT(type_ == val.GetTypeId(), "Mismatched types");
  SetNull(index, val.IsNull());
  const uint64_t actual_index = sel_vector_ != nullptr ? sel_vector_[index] : index;
  switch (type_) {
    case TypeId::Boolean: {
      const auto new_boolean = val.IsNull() ? false : val.value_.boolean;
      reinterpret_cast<bool *>(data_)[actual_index] = new_boolean;
      break;
    }
    case TypeId::TinyInt: {
      const auto new_tinyint = val.IsNull() ? 0 : val.value_.tinyint;
      reinterpret_cast<int8_t *>(data_)[actual_index] = new_tinyint;
      break;
    }
    case TypeId::SmallInt: {
      const auto new_smallint = val.IsNull() ? 0 : val.value_.smallint;
      reinterpret_cast<int16_t *>(data_)[actual_index] = new_smallint;
      break;
    }
    case TypeId::Integer: {
      const auto new_integer = val.IsNull() ? 0 : val.value_.integer;
      reinterpret_cast<int32_t *>(data_)[actual_index] = new_integer;
      break;
    }
    case TypeId::BigInt: {
      const auto new_bigint = val.IsNull() ? 0 : val.value_.bigint;
      reinterpret_cast<int64_t *>(data_)[actual_index] = new_bigint;
      break;
    }
    case TypeId::Float: {
      const auto new_float = val.IsNull() ? 0 : val.value_.float_;
      reinterpret_cast<float *>(data_)[actual_index] = new_float;
      break;
    }
    case TypeId::Double: {
      const auto new_double = val.IsNull() ? 0 : val.value_.double_;
      reinterpret_cast<double *>(data_)[actual_index] = new_double;
      break;
    }
    case TypeId::Date: {
      const auto new_date = val.IsNull() ? Date() : val.value_.date_;
      reinterpret_cast<Date *>(data_)[actual_index] = new_date;
      break;
    }
    case TypeId::Hash: {
      const auto new_hash = val.IsNull() ? 0 : val.value_.hash;
      reinterpret_cast<hash_t *>(data_)[actual_index] = new_hash;
      break;
    }
    case TypeId::Pointer: {
      const auto new_pointer = val.IsNull() ? 0 : val.value_.pointer;
      reinterpret_cast<uintptr_t *>(data_)[actual_index] = new_pointer;
      break;
    }
    case TypeId::Varchar: {
      if (!val.IsNull()) {
        reinterpret_cast<VarlenEntry *>(data_)[actual_index] = varlens_.AddVarlen(val.str_value_);
      }
      break;
    }
    default: {
      throw std::runtime_error(
          fmt::format("Cannot write value of type '{}' into vector", TypeIdToString(type_)));
    }
  }
}

void Vector::Reference(GenericValue *value) {
  // Cleanup
  Destroy();

  // Start from scratch
  type_ = value->GetTypeId();
  num_elems_ = count_ = 1;
  null_mask_.Resize(num_elems_);

  if (value->IsNull()) {
    SetNull(0, true);
  }

  switch (value->GetTypeId()) {
    case TypeId::Boolean: {
      data_ = reinterpret_cast<byte *>(&value->value_.boolean);
      break;
    }
    case TypeId::TinyInt: {
      data_ = reinterpret_cast<byte *>(&value->value_.tinyint);
      break;
    }
    case TypeId::SmallInt: {
      data_ = reinterpret_cast<byte *>(&value->value_.smallint);
      break;
    }
    case TypeId::Integer: {
      data_ = reinterpret_cast<byte *>(&value->value_.integer);
      break;
    }
    case TypeId::BigInt: {
      data_ = reinterpret_cast<byte *>(&value->value_.bigint);
      break;
    }
    case TypeId::Float: {
      data_ = reinterpret_cast<byte *>(&value->value_.float_);
      break;
    }
    case TypeId::Double: {
      data_ = reinterpret_cast<byte *>(&value->value_.double_);
      break;
    }
    case TypeId::Date: {
      data_ = reinterpret_cast<byte *>(&value->value_.date_);
      break;
    }
    case TypeId::Hash: {
      data_ = reinterpret_cast<byte *>(&value->value_.hash);
      break;
    }
    case TypeId::Pointer: {
      data_ = reinterpret_cast<byte *>(&value->value_.pointer);
      break;
    }
    case TypeId::Varchar: {
      owned_data_ = std::make_unique<byte[]>(sizeof(VarlenEntry));
      data_ = owned_data_.get();
      auto *content = const_cast<byte *>(reinterpret_cast<const byte *>(value->str_value_.c_str()));
      reinterpret_cast<VarlenEntry *>(data_)[0] =
          VarlenEntry::Create(content, value->str_value_.size());
      break;
    }
    default: {
      throw std::runtime_error(
          fmt::format("Cannot read value of type '{}'", TypeIdToString(type_)));
    }
  }
}

void Vector::Reference(byte *data, uint32_t *nullmask, uint64_t size) {
  TPL_ASSERT(owned_data_ == nullptr, "Cannot reference a vector if owning data");
  count_ = size;
  num_elems_ = size;
  data_ = data;
  sel_vector_ = nullptr;
  null_mask_.Resize(num_elems_);

  // TODO(pmenon): Optimize me if this is a bottleneck
  if (nullmask == nullptr) {
    null_mask_.Reset();
  } else {
    for (uint64_t i = 0; i < size; i++) {
      null_mask_[i] = util::BitUtil::Test(nullmask, i);
    }
  }
}

void Vector::Reference(Vector *other) {
  TPL_ASSERT(owned_data_ == nullptr, "Cannot reference a vector if owning data");
  type_ = other->type_;
  count_ = other->count_;
  num_elems_ = other->num_elems_;
  data_ = other->data_;
  sel_vector_ = other->sel_vector_;
  null_mask_ = other->null_mask_;
}

void Vector::MoveTo(Vector *other) {
  other->Destroy();
  other->type_ = type_;
  other->count_ = count_;
  other->num_elems_ = num_elems_;
  other->data_ = data_;
  other->sel_vector_ = sel_vector_;
  other->null_mask_ = null_mask_;
  other->owned_data_ = move(owned_data_);
  other->varlens_ = std::move(varlens_);

  // Cleanup
  Destroy();
}

void Vector::CopyTo(Vector *other, uint64_t offset) {
  TPL_ASSERT(type_ == other->type_,
             "Copying to vector of different type. Did you mean to cast instead?");
  TPL_ASSERT(other->sel_vector_ == nullptr,
             "Copying to a vector with a selection vector isn't supported");

  other->GetMutableNullMask()->Reset();

  if (IsTypeFixedSize(type_)) {
    VectorOps::Copy(*this, other, offset);
  } else {
    TPL_ASSERT(type_ == TypeId::Varchar, "Wrong type for copy");
    other->Resize(count_ - offset);
    auto src_data = reinterpret_cast<const VarlenEntry *>(data_);
    auto target_data = reinterpret_cast<VarlenEntry *>(other->data_);
    VectorOps::Exec(*this,
                    [&](uint64_t i, uint64_t k) {
                      if (null_mask_[i]) {
                        other->null_mask_.Set(k - offset);
                      } else {
                        target_data[k - offset] = other->varlens_.AddVarlen(src_data[i]);
                      }
                    },
                    offset);
  }
}

void Vector::Cast(TypeId new_type) {
  if (type_ == new_type) {
    return;
  }

  Vector new_vector(new_type, true, false);
  VectorOps::Cast(*this, &new_vector);
  new_vector.MoveTo(this);
}

void Vector::Append(const Vector &other) {
  TPL_ASSERT(sel_vector_ == nullptr, "Appending to vector with selection vector not supported");
  TPL_ASSERT(type_ == other.type_, "Can only append vector of same type");

  if (GetSize() + other.GetCount() > kDefaultVectorSize) {
    throw std::out_of_range("Cannot append to vector: vector is too large");
  }

  uint64_t old_size = count_;
  num_elems_ += other.GetCount();
  count_ += other.GetCount();

  // Since the vector's size has changed, we need to also resize the NULL bitmask.
  null_mask_.Resize(num_elems_);

  // merge NULL mask
  VectorOps::Exec(other,
                  [&](uint64_t i, uint64_t k) { null_mask_[old_size + k] = other.null_mask_[i]; });

  if (IsTypeFixedSize(type_)) {
    VectorOps::Copy(other, data_ + old_size * GetTypeIdSize(type_));
  } else {
    TPL_ASSERT(type_ == TypeId::Varchar, "Append on varchars");
    auto src_data = reinterpret_cast<const VarlenEntry *>(other.data_);
    auto target_data = reinterpret_cast<VarlenEntry *>(data_);
    VectorOps::Exec(other, [&](uint64_t i, uint64_t k) {
      if (other.null_mask_[i]) {
      } else {
        target_data[old_size + k] = varlens_.AddVarlen(src_data[i]);
      }
    });
  }
}

std::string Vector::ToString() const {
  std::string result = TypeIdToString(type_) + "=[";
  bool first = true;
  for (uint64_t i = 0; i < GetCount(); i++) {
    if (!first) result += ",";
    first = false;
    result += GetValue(i).ToString();
  }
  result += "]";
  return result;
}

void Vector::Dump(std::ostream &stream) const { stream << ToString() << std::endl; }

void Vector::CheckIntegrity() const {
#ifndef NDEBUG
  // Ensure that if there isn't a selection vector, the size and selected count values are equal
  if (sel_vector_ == nullptr) {
    TPL_ASSERT(count_ == num_elems_,
               "Vector count() and num_elems() do not match when missing selection vector");
  }

  // Ensure that the NULL bitmask has the same size at the vector it represents
  TPL_ASSERT(num_elems_ == null_mask_.num_bits(), "NULL bitmask size doesn't match vector size");

  // Check the strings in the vector, if it's a string vector
  if (type_ == TypeId::Varchar) {
    VectorOps::ExecTyped<const VarlenEntry>(
        *this, [&](const VarlenEntry &varlen, uint64_t i, uint64_t k) {
          if (!null_mask_[i]) {
            TPL_ASSERT(varlen.GetContent() != nullptr, "NULL pointer in non-null vector slot");
          }
        });
  }
#endif
}

}  // namespace tpl::sql
