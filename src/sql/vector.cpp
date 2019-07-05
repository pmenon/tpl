#include "sql/vector.h"

#include <cstring>
#include <iostream>
#include <util/bit_util.h>

#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Strings
// ---------------------------------------------------------

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

// ---------------------------------------------------------
// Vector
// ---------------------------------------------------------

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
    auto src_data = reinterpret_cast<const char **>(other.data_);
    auto target_data = reinterpret_cast<const char **>(data_);
    VectorOps::Exec(other, [&](u64 i, u64 k) {
      if (other.null_mask_[i]) {
        target_data[old_count + k] = nullptr;
      } else {
        target_data[old_count + k] = strings_.AddString(src_data[i]);
      }
    });
  }
}

namespace {

struct StringifyVal {
  template <typename T>
  static std::string Apply(T val) {
    return std::to_string(val);
  }
};

template <>
std::string StringifyVal::Apply(bool val) {
  return val ? "True" : "False";
}

template <>
std::string StringifyVal::Apply(const char *val) {
  return "Varchar('" + std::string(val) + "')";
}

template <>
std::string StringifyVal::Apply(Blob val) {
  return "Blob(sz=" + std::to_string(val.size) + ")";
}

template <typename T>
std::string Stringify(const Vector &vec) {
  std::string result;
  bool first = true;
  const auto *data = reinterpret_cast<T *>(vec.data());
  VectorOps::Exec(vec, [&](u64 i, u64 k) {
    if (!first) result += ",";
    first = false;
    if (vec.null_mask()[i]) {
      result += "NULL";
    } else {
      result += StringifyVal::Apply(data[i]);
    }
  });
  return result;
}

}  // namespace

std::string Vector::ToString() const {
  switch (type_) {
    case TypeId::Boolean:
      return Stringify<bool>(*this);
    case TypeId::TinyInt:
      return Stringify<i8>(*this);
    case TypeId::SmallInt:
      return Stringify<i16>(*this);
    case TypeId::Integer:
      return Stringify<i32>(*this);
    case TypeId::BigInt:
      return Stringify<i64>(*this);
    case TypeId::Hash:
      return Stringify<hash_t>(*this);
    case TypeId::Pointer:
      return Stringify<uintptr_t>(*this);
    case TypeId::Float:
      return Stringify<f32>(*this);
    case TypeId::Double:
      return Stringify<f64>(*this);
    case TypeId::Varchar:
      return Stringify<const char *>(*this);
    case TypeId::Varbinary:
      return Stringify<Blob>(*this);
    default:
      UNREACHABLE("Impossible primitive type");
  }
}

void Vector::Dump(std::ostream &os) const { os << ToString(); }

}  // namespace tpl::sql
