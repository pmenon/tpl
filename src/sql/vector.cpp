#include "sql/vector.h"

#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/vector_operations/vector_operations.h"
#include "util/bit_util.h"

namespace tpl::sql {

Vector::Vector(TypeId type)
    : type_(type), count_(0), num_elements_(0), data_(nullptr), tid_list_(nullptr) {
  // Since vector capacity can never exceed kDefaultVectorSize, we reserve upon
  // creation to remove allocations as the vector is resized.
  null_mask_.Reserve(kDefaultVectorSize);
  null_mask_.Resize(num_elements_);
}

Vector::Vector(TypeId type, bool create_data, bool clear)
    : type_(type), count_(0), num_elements_(0), data_(nullptr), tid_list_(nullptr) {
  // Since vector capacity can never exceed kDefaultVectorSize, we reserve upon
  // creation to remove allocations as the vector is resized.
  null_mask_.Reserve(kDefaultVectorSize);
  null_mask_.Resize(num_elements_);
  if (create_data) {
    Initialize(type, clear);
  }
}

Vector::~Vector() { Destroy(); }

void Vector::Initialize(const TypeId new_type, const bool clear) {
  varlen_heap_.Destroy();
  type_ = new_type;

  // By default, we always allocate ::tpl::kDefaultVectorSize since a vector
  // can never exceed this capacity during query processing.
  const std::size_t num_bytes = kDefaultVectorSize * GetTypeIdSize(type_);
  owned_data_ = std::unique_ptr<byte[]>(new byte[num_bytes]);
  data_ = owned_data_.get();

  if (clear) {
    std::memset(data_, 0, num_bytes);
  }
}

void Vector::Destroy() {
  owned_data_.reset();
  varlen_heap_.Destroy();
  data_ = nullptr;
  count_ = 0;
  num_elements_ = 0;
  tid_list_ = nullptr;
  null_mask_.Reset();
}

GenericValue Vector::GetValue(const uint32_t index) const {
  TPL_ASSERT(index < count_, "Out-of-bounds vector access");
  if (IsNull(index)) {
    return GenericValue::CreateNull(type_);
  }
  auto actual_index = (tid_list_ != nullptr ? (*tid_list_)[index] : index);
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
      throw NotImplementedException(
          fmt::format("cannot read vector value of type '{}'", TypeIdToString(type_)));
    }
  }
}

void Vector::Resize(uint32_t size) {
  TPL_ASSERT(size <= GetCapacity(), "New size exceeds vector capacity.");
  tid_list_ = nullptr;
  count_ = size;
  num_elements_ = size;
  null_mask_.Resize(num_elements_);
}

void Vector::SetValue(const uint32_t index, const GenericValue &val) {
  TPL_ASSERT(index < count_, "Out-of-bounds vector access");
  TPL_ASSERT(type_ == val.GetTypeId(), "Mismatched types");
  SetNull(index, val.IsNull());
  const uint32_t actual_index = tid_list_ != nullptr ? (*tid_list_)[index] : index;
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
        reinterpret_cast<VarlenEntry *>(data_)[actual_index] =
            varlen_heap_.AddVarlen(val.str_value_);
      }
      break;
    }
    default: {
      throw NotImplementedException(
          fmt::format("cannot write vector value of type '{}'", TypeIdToString(type_)));
    }
  }
}

void Vector::Reference(GenericValue *value) {
  // Cleanup
  Destroy();

  // Start from scratch
  type_ = value->GetTypeId();
  num_elements_ = count_ = 1;
  null_mask_.Resize(num_elements_);

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
      throw NotImplementedException(
          fmt::format("Cannot reference vector of type '{}'", TypeIdToString(type_)));
    }
  }
}

void Vector::Reference(byte *data, const uint32_t *null_mask, uint32_t size) {
  TPL_ASSERT(owned_data_ == nullptr, "Cannot reference a vector if owning data");
  count_ = size;
  num_elements_ = size;
  data_ = data;
  tid_list_ = nullptr;
  null_mask_.Resize(num_elements_);

  // TODO(pmenon): Optimize me if this is a bottleneck
  if (null_mask == nullptr) {
    null_mask_.Reset();
  } else {
    for (uint32_t i = 0; i < size; i++) {
      null_mask_[i] = util::BitUtil::Test(null_mask, i);
    }
  }
}

void Vector::Reference(const Vector *other) {
  TPL_ASSERT(owned_data_ == nullptr, "Cannot reference a vector if owning data");
  type_ = other->type_;
  count_ = other->count_;
  num_elements_ = other->num_elements_;
  data_ = other->data_;
  tid_list_ = other->tid_list_;
  null_mask_ = other->null_mask_;
}

void Vector::Pack() {
  if (tid_list_ == nullptr) {
    return;
  }

  Vector copy(GetTypeId(), true, false);
  CopyTo(&copy);
  copy.MoveTo(this);
}

void Vector::GetNonNullSelections(TupleIdList *non_null_tids, TupleIdList *null_tids) const {
  non_null_tids->Resize(GetSize());
  null_tids->Resize(GetSize());

  // Copy NULLs directly
  null_tids->GetMutableBits()->Copy(null_mask_);

  // Copy selections
  if (tid_list_ != nullptr) {
    non_null_tids->AssignFrom(*tid_list_);
  } else {
    non_null_tids->AddAll();
  }

  // Ensure NULL list only refers to selected TIDs
  null_tids->GetMutableBits()->Intersect(*non_null_tids->GetMutableBits());

  // Remove NULLs from filtered TIDs
  non_null_tids->GetMutableBits()->Difference(null_mask_);
}

void Vector::MoveTo(Vector *other) {
  other->Destroy();
  other->type_ = type_;
  other->count_ = count_;
  other->num_elements_ = num_elements_;
  other->data_ = data_;
  other->tid_list_ = tid_list_;
  other->null_mask_ = std::move(null_mask_);
  other->owned_data_ = std::move(owned_data_);
  other->varlen_heap_ = std::move(varlen_heap_);

  // Cleanup
  Destroy();
}

void Vector::Clone(Vector *target) {
  TPL_ASSERT(target->owned_data_ != nullptr, "Cannot clone into a reference vector");
  target->Resize(GetSize());
  target->type_ = type_;
  target->count_ = count_;
  target->num_elements_ = num_elements_;
  target->tid_list_ = tid_list_;
  target->null_mask_.Copy(null_mask_);

  if (IsTypeFixedSize(type_)) {
    std::memcpy(target->GetData(), GetData(), GetTypeIdSize(type_) * num_elements_);
  } else {
    auto src_data = reinterpret_cast<const VarlenEntry *>(data_);
    auto target_data = reinterpret_cast<VarlenEntry *>(target->data_);
    VectorOps::Exec(*this, [&](auto i, auto k) {
      if (!null_mask_[i]) {
        target_data[i] = target->varlen_heap_.AddVarlen(src_data[i]);
      }
    });
  }
}

void Vector::CopyTo(Vector *other) {
  TPL_ASSERT(type_ == other->type_, "Vector type mismatch in CopyTo(). Did you mean to Cast()?");
  TPL_ASSERT(other->tid_list_ == nullptr, "Cannot copy into a filtered vector.");
  VectorOps::Copy(*this, other);
}

void Vector::Cast(TypeId new_type) {
  if (type_ == new_type) return;

  Vector new_vector(new_type, true, false);
  VectorOps::Cast(*this, &new_vector);
  new_vector.MoveTo(this);
}

std::string Vector::ToString() const {
  std::string result = TypeIdToString(type_) + "=[";
  bool first = true;
  for (uint32_t i = 0; i < GetCount(); i++) {
    if (!first) result += ",";
    first = false;
    result += GetValue(i).ToString();
  }
  result += "]";
  return result;
}

void Vector::Dump(std::ostream &os) const { os << ToString() << std::endl; }

void Vector::CheckIntegrity() const {
#ifndef NDEBUG
  // Ensure TID list shape
  if (tid_list_ == nullptr) {
    TPL_ASSERT(count_ == num_elements_, "Vector count and size do not match in unfiltered vector");
  } else {
    TPL_ASSERT(num_elements_ == tid_list_->GetCapacity(),
               "TID list too small to capture all vector elements");
    TPL_ASSERT(count_ == tid_list_->GetTupleCount(), "TID list size and cached count do not match");
    TPL_ASSERT(count_ <= num_elements_,
               "Vector count must be smaller than size with selection vector");
  }

  // Ensure that the NULL bit mask has the same size at the vector it represents
  TPL_ASSERT(num_elements_ == null_mask_.GetNumBits(),
             "NULL indication bit vector size doesn't match vector size");

  // Check the strings in the vector, if it's a string vector
  if (type_ == TypeId::Varchar) {
    VectorOps::ExecTyped<const VarlenEntry>(*this, [&](const auto &varlen, auto i, auto k) {
      if (!null_mask_[i]) {
        TPL_ASSERT(varlen.GetContent() != nullptr, "NULL pointer in non-null vector slot");
      }
    });
  }
#endif
}

}  // namespace tpl::sql
