#include "sql/vector.h"

#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "logging/logger.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/bit_util.h"

namespace tpl::sql {

//===----------------------------------------------------------------------===//
//
// Strings
//
//===----------------------------------------------------------------------===//

Vector::Strings::Strings() : region_("vector-strings"), num_strings_(0) {}

char *Vector::Strings::AddString(const std::string_view str) {
  // Track the number of strings
  num_strings_++;

  // Allocate string-length bytes + 1 for the NULL terminator
  const auto num_bytes = str.length() + 1;
  auto ptr = region_.Allocate(num_bytes, alignof(char *));
  std::memcpy(ptr, str.data(), num_bytes);

  // Done
  return reinterpret_cast<char *>(ptr);
}

void Vector::Strings::Destroy() { region_.FreeAll(); }

//===----------------------------------------------------------------------===//
//
// Vector
//
//===----------------------------------------------------------------------===//

Vector::Vector(TypeId type)
    : type_(type),
      count_(0),
      data_(nullptr),
      sel_vector_(nullptr),
      null_mask_(0) {}

Vector::Vector(TypeId type, byte *data, u64 count)
    : type_(type),
      count_(count),
      data_(data),
      sel_vector_(nullptr),
      null_mask_(0) {
  TPL_ASSERT(data != nullptr, "Cannot create vector from NULL data pointer");
}

Vector::Vector(TypeId type, bool create_data, bool clear)
    : type_(type), count_(0), data_(nullptr), sel_vector_(nullptr) {
  if (create_data) {
    Initialize(type, clear);
  }
}

Vector::~Vector() { Destroy(); }

void Vector::Initialize(const TypeId new_type, const bool clear) {
  type_ = new_type;
  strings_.Destroy();
  const auto num_bytes = kDefaultVectorSize * GetTypeIdSize(type_);
  owned_data_ = std::make_unique<byte[]>(num_bytes);
  data_ = owned_data_.get();
  if (clear) {
    std::memset(data_, 0, num_bytes);
  }
}

void Vector::Destroy() {
  owned_data_.reset();
  strings_.Destroy();
  data_ = nullptr;
  count_ = 0;
  sel_vector_ = nullptr;
  null_mask_.reset();
}

GenericValue Vector::GetValue(const u64 index) const {
  TPL_ASSERT(index <= count_, "Out-of-bounds vector access");
  if (IsNull(index)) {
    return GenericValue::CreateNull(type_);
  }
  auto actual_index = (sel_vector_ != nullptr ? sel_vector_[index] : index);
  switch (type_) {
    case TypeId::Boolean: {
      return GenericValue::CreateBoolean(
          reinterpret_cast<bool *>(data_)[actual_index]);
    }
    case TypeId::TinyInt: {
      return GenericValue::CreateTinyInt(
          reinterpret_cast<i8 *>(data_)[actual_index]);
    }
    case TypeId::SmallInt: {
      return GenericValue::CreateSmallInt(
          reinterpret_cast<i16 *>(data_)[actual_index]);
    }
    case TypeId::Integer: {
      return GenericValue::CreateInteger(
          reinterpret_cast<i32 *>(data_)[actual_index]);
    }
    case TypeId::BigInt: {
      return GenericValue::CreateBigInt(
          reinterpret_cast<i64 *>(data_)[actual_index]);
    }
    case TypeId::Hash: {
      return GenericValue::CreateHash(
          reinterpret_cast<hash_t *>(data_)[actual_index]);
    }
    case TypeId::Pointer: {
      return GenericValue::CreatePointer(
          reinterpret_cast<uintptr_t *>(data_)[actual_index]);
    }
    case TypeId::Float: {
      return GenericValue::CreateReal(
          reinterpret_cast<f32 *>(data_)[actual_index]);
    }
    case TypeId::Double: {
      return GenericValue::CreateDouble(
          reinterpret_cast<f64 *>(data_)[actual_index]);
    }
    case TypeId::Varchar: {
      auto *str = reinterpret_cast<const char **>(data_)[actual_index];
      TPL_ASSERT(str != nullptr, "Null string in position not marked NULL!");
      return GenericValue::CreateVarchar(str);
    }
    default: {
      throw std::runtime_error(fmt::format(
          "Cannot read value of type '{}' from vector", TypeIdToString(type_)));
    }
  }
}

void Vector::SetValue(const u64 index, const GenericValue &val) {
  TPL_ASSERT(index <= count_, "Out-of-bounds vector access");
  TPL_ASSERT(type_ == val.type_id(), "Mismatched types");
  SetNull(index, val.is_null());
  const u64 actual_index = sel_vector_ != nullptr ? sel_vector_[index] : index;
  switch (type_) {
    case TypeId::Boolean: {
      const auto new_boolean = val.is_null() ? false : val.value_.boolean;
      reinterpret_cast<bool *>(data_)[actual_index] = new_boolean;
      break;
    }
    case TypeId::TinyInt: {
      const auto new_tinyint = val.is_null() ? 0 : val.value_.tinyint;
      reinterpret_cast<i8 *>(data_)[actual_index] = new_tinyint;
      break;
    }
    case TypeId::SmallInt: {
      const auto new_smallint = val.is_null() ? 0 : val.value_.smallint;
      reinterpret_cast<i16 *>(data_)[actual_index] = new_smallint;
      break;
    }
    case TypeId::Integer: {
      const auto new_integer = val.is_null() ? 0 : val.value_.integer;
      reinterpret_cast<i32 *>(data_)[actual_index] = new_integer;
      break;
    }
    case TypeId::BigInt: {
      const auto new_bigint = val.is_null() ? 0 : val.value_.bigint;
      reinterpret_cast<i64 *>(data_)[actual_index] = new_bigint;
      break;
    }
    case TypeId::Float: {
      const auto new_float = val.is_null() ? 0 : val.value_.float_;
      reinterpret_cast<f32 *>(data_)[actual_index] = new_float;
      break;
    }
    case TypeId::Double: {
      const auto new_double = val.is_null() ? 0 : val.value_.double_;
      reinterpret_cast<f64 *>(data_)[actual_index] = new_double;
      break;
    }
    case TypeId::Hash: {
      const auto new_hash = val.is_null() ? 0 : val.value_.hash;
      reinterpret_cast<hash_t *>(data_)[actual_index] = new_hash;
      break;
    }
    case TypeId::Pointer: {
      const auto new_pointer = val.is_null() ? 0 : val.value_.pointer;
      reinterpret_cast<uintptr_t *>(data_)[actual_index] = new_pointer;
      break;
    }
    case TypeId::Varchar: {
      auto str = (val.is_null() ? nullptr : strings_.AddString(val.str_value_));
      reinterpret_cast<const char **>(data_)[actual_index] = str;
      break;
    }
    default: {
      throw std::runtime_error(
          fmt::format("Cannot write value of type '{}' into vector",
                      TypeIdToString(type_)));
    }
  }
}

void Vector::Reference(GenericValue *value) {
  // Cleanup
  Destroy();

  // Start from scratch
  type_ = value->type_id();
  count_ = 1;

  if (value->is_null()) {
    null_mask_[0] = true;
  }

  switch (value->type_id()) {
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
    case TypeId::Hash: {
      data_ = reinterpret_cast<byte *>(&value->value_.hash);
      break;
    }
    case TypeId::Pointer: {
      data_ = reinterpret_cast<byte *>(&value->value_.pointer);
      break;
    }
    case TypeId::Varchar: {
      // Single-element array
      owned_data_ = std::make_unique<byte[]>(sizeof(byte *));
      data_ = owned_data_.get();
      reinterpret_cast<const char **>(data_)[0] = value->str_value_.c_str();
      break;
    }
    default: {
      throw std::runtime_error(
          fmt::format("Cannot read value of type '{}'", TypeIdToString(type_)));
    }
  }
}

void Vector::Reference(TypeId type_id, byte *data, u32 *nullmask, u64 count) {
  TPL_ASSERT(owned_data_ == nullptr,
             "Cannot reference a vector if owning data");
  count_ = count;
  data_ = data;
  sel_vector_ = nullptr;
  type_ = type_id;

  // TODO(pmenon): Optimize me if this is a bottleneck
  if (nullmask == nullptr) {
    null_mask_.reset();
  } else {
    for (u32 i = 0; i < count; i++) {
      null_mask_[i] = util::BitUtil::Test(nullmask, i);
    }
  }
}

void Vector::Reference(Vector *other) {
  TPL_ASSERT(owned_data_ == nullptr,
             "Cannot reference a vector if owning data");
  count_ = other->count_;
  data_ = other->data_;
  sel_vector_ = other->sel_vector_;
  type_ = other->type_;
  null_mask_ = other->null_mask_;
}

void Vector::MoveTo(Vector *other) {
  other->Destroy();
  other->owned_data_ = move(owned_data_);
  other->strings_ = std::move(strings_);
  other->count_ = count_;
  other->data_ = data_;
  other->sel_vector_ = sel_vector_;
  other->type_ = type_;
  other->null_mask_ = null_mask_;

  // Cleanup
  Destroy();
}

void Vector::Flatten() {
  // If there's no selection vector, nothing to do since we're already flat
  if (sel_vector_ == nullptr) {
    return;
  }

  Vector other(type_, true, false);
  CopyTo(&other);
  other.MoveTo(this);
}

void Vector::CopyTo(Vector *other, u64 offset) {
  TPL_ASSERT(
      type_ == other->type_,
      "Copying to vector of different type. Did you mean to cast instead?");
  TPL_ASSERT(other->sel_vector_ == nullptr,
             "Copying to a vector with a selection vector isn't supported");

  other->null_mask_.reset();

  if (IsTypeFixedSize(type_)) {
    VectorOps::Copy(*this, other, offset);
  } else {
    TPL_ASSERT(type_ == TypeId::Varchar, "Wrong type for copy");
    other->count_ = count_ - offset;
    auto src_data = reinterpret_cast<const char **>(data_);
    auto target_data = reinterpret_cast<const char **>(other->data_);
    VectorOps::Exec(*this,
                    [&](u64 i, u64 k) {
                      if (null_mask_[i]) {
                        other->null_mask_[k - offset] = true;
                        target_data[k - offset] = nullptr;
                      } else {
                        target_data[k - offset] =
                            other->strings_.AddString(src_data[i]);
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

void Vector::Append(Vector &other) {
  TPL_ASSERT(sel_vector_ == nullptr,
             "Appending to vector with selection vector not supported");
  TPL_ASSERT(type_ == other.type_, "Can only append vector of same type");

  if (count_ + other.count_ > kDefaultVectorSize) {
    throw std::out_of_range("Cannot append to vector: vector is too large");
  }

  u64 old_count = count_;
  count_ += other.count_;

  // merge NULL mask
  VectorOps::Exec(other, [&](u64 i, u64 k) {
    null_mask_[old_count + k] = other.null_mask_[i];
  });

  if (IsTypeFixedSize(type_)) {
    VectorOps::Copy(other, data_ + old_count * GetTypeIdSize(type_));
  } else {
    TPL_ASSERT(type_ == TypeId::Varchar, "Append on varchars");
    auto src_data = reinterpret_cast<const char **const>(other.data_);
    auto target_data = reinterpret_cast<const char **const>(data_);
    VectorOps::Exec(other, [&](u64 i, u64 k) {
      if (other.null_mask_[i]) {
        target_data[old_count + k] = nullptr;
      } else {
        target_data[old_count + k] = strings_.AddString(src_data[i]);
      }
    });
  }
}

std::string Vector::ToString() const {
  std::string result = std::string(TypeIdToString(type_)) + "=[";
  bool first = true;
  VectorOps::Exec(*this, [&](u64 i, u64 k) {
    if (!first) result += ",";
    first = false;
    result += GetValue(i).ToString();
  });
  result += "]";
  return result;
}

void Vector::Dump(std::ostream &stream) const {
  stream << ToString() << std::endl;
}

void Vector::CheckIntegrity() const {
#ifndef NDEBUG
  if (type_ == TypeId::Varchar) {
    VectorOps::ExecTyped<const char *>(*this, [&](const char *string, u64 i,
                                                  u64 k) {
      if (!null_mask_[i]) {
        TPL_ASSERT(string != nullptr, "NULL pointer in non-null vector slot");
        // The following check is unsafe. But, we're in debug mode presumably
        // with ASAN on, so a corrupt string will trigger an ASAN fault.
        TPL_ASSERT(
            std::strlen(string) < std::numeric_limits<std::size_t>::max(),
            "Invalid string length");
      }
    });
  }
#endif
}

}  // namespace tpl::sql
