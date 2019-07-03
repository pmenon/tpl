#include "sql/data_types.h"

#include <memory>
#include <string>
#include <utility>

#include "llvm/ADT/DenseMap.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Boolean
// ---------------------------------------------------------

BooleanType::BooleanType(bool nullable)
    : SqlType(SqlTypeId::Boolean, nullable) {}

std::string BooleanType::GetName() const {
  std::string str = "Boolean";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool BooleanType::IsArithmetic() const { return false; }

bool BooleanType::Equals(const SqlType &other) const {
  return other.Is<BooleanType>() && nullable() == other.nullable();
}

const BooleanType &BooleanType::InstanceNonNullable() {
  static BooleanType kNonNullableBoolean(false);
  return kNonNullableBoolean;
}

const BooleanType &BooleanType::InstanceNullable() {
  static BooleanType kNullableBoolean(true);
  return kNullableBoolean;
}

// ---------------------------------------------------------
// Tiny Integer
// ---------------------------------------------------------

TinyIntType::TinyIntType(bool nullable)
    : NumberBaseType(SqlTypeId::TinyInt, nullable) {}

std::string TinyIntType::GetName() const {
  std::string str = "TinyInt";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool TinyIntType::Equals(const SqlType &other) const {
  return other.Is<TinyIntType>() && nullable() == other.nullable();
}

const TinyIntType &TinyIntType::InstanceNonNullable() {
  static TinyIntType kNonNullableTinyInt(false);
  return kNonNullableTinyInt;
}

const TinyIntType &TinyIntType::InstanceNullable() {
  static TinyIntType kNullableTinyInt(true);
  return kNullableTinyInt;
}

// ---------------------------------------------------------
// Small Integer
// ---------------------------------------------------------

SmallIntType::SmallIntType(bool nullable)
    : NumberBaseType(SqlTypeId::SmallInt, nullable) {}

std::string SmallIntType::GetName() const {
  std::string str = "SmallInt";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool SmallIntType::Equals(const SqlType &other) const {
  return other.Is<SmallIntType>() && nullable() == other.nullable();
}

const SmallIntType &SmallIntType::InstanceNonNullable() {
  static SmallIntType kNonNullableSmallInt(false);
  return kNonNullableSmallInt;
}

const SmallIntType &SmallIntType::InstanceNullable() {
  static SmallIntType kNullableSmallInt(true);
  return kNullableSmallInt;
}

// ---------------------------------------------------------
// Integer
// ---------------------------------------------------------

IntegerType::IntegerType(bool nullable)
    : NumberBaseType(SqlTypeId::Integer, nullable) {}

std::string IntegerType::GetName() const {
  std::string str = "Integer";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool IntegerType::Equals(const SqlType &other) const {
  return other.Is<IntegerType>() && nullable() == other.nullable();
}

const IntegerType &IntegerType::InstanceNonNullable() {
  static IntegerType kNonNullableInt(false);
  return kNonNullableInt;
}

const IntegerType &IntegerType::InstanceNullable() {
  static IntegerType kNullableInt(true);
  return kNullableInt;
}

// ---------------------------------------------------------
// Big Integer
// ---------------------------------------------------------

BigIntType::BigIntType(bool nullable)
    : NumberBaseType(SqlTypeId::BigInt, nullable) {}

std::string BigIntType::GetName() const {
  std::string str = "BigInt";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool BigIntType::Equals(const SqlType &other) const {
  return other.Is<BigIntType>() && nullable() == other.nullable();
}

const BigIntType &BigIntType::InstanceNonNullable() {
  static BigIntType kNonNullableBigInt(false);
  return kNonNullableBigInt;
}

const BigIntType &BigIntType::InstanceNullable() {
  static BigIntType kNullableBigInt(true);
  return kNullableBigInt;
}

// ---------------------------------------------------------
// Real
// ---------------------------------------------------------

RealType::RealType(bool nullable) : NumberBaseType(SqlTypeId::Real, nullable) {}

std::string RealType::GetName() const {
  std::string str = "Real";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool RealType::Equals(const SqlType &other) const {
  return other.Is<RealType>() && nullable() == other.nullable();
}

const RealType &RealType::InstanceNonNullable() {
  static RealType kNonNullableBigInt(false);
  return kNonNullableBigInt;
}

const RealType &RealType::InstanceNullable() {
  static RealType kNullableBigInt(true);
  return kNullableBigInt;
}

// ---------------------------------------------------------
// Decimal
// ---------------------------------------------------------

DecimalType::DecimalType(bool nullable, u32 precision, u32 scale)
    : SqlType(SqlTypeId::Decimal, nullable),
      precision_(precision),
      scale_(scale) {}

std::string DecimalType::GetName() const {
  std::string str =
      "Decimal[" + std::to_string(precision()) + "," + std::to_string(scale());
  if (nullable()) {
    str.append(",NULLABLE");
  }
  str.append("]");
  return str;
}

bool DecimalType::Equals(const SqlType &other) const {
  if (auto *other_decimal = other.SafeAs<DecimalType>()) {
    return precision() == other_decimal->precision() &&
           scale() == other_decimal->scale() && nullable() == other.nullable();
  }
  return false;
}

bool DecimalType::IsArithmetic() const { return true; }

u32 DecimalType::precision() const { return precision_; }

u32 DecimalType::scale() const { return scale_; }

template <bool Nullable>
const DecimalType &DecimalType::InstanceInternal(u32 precision, u32 scale) {
  static llvm::DenseMap<std::pair<u32, u32>, std::unique_ptr<DecimalType>>
      kDecimalTypeMap;

  auto key = std::make_pair(precision, scale);
  if (auto iter = kDecimalTypeMap.find(key); iter != kDecimalTypeMap.end()) {
    return *iter->second;
  }

  auto iter = kDecimalTypeMap.try_emplace(
      key, new DecimalType(Nullable, precision, scale));
  return *iter.first->second;
}

const DecimalType &DecimalType::InstanceNonNullable(u32 precision, u32 scale) {
  return InstanceInternal<false>(precision, scale);
}

const DecimalType &DecimalType::InstanceNullable(u32 precision, u32 scale) {
  return InstanceInternal<true>(precision, scale);
}

// ---------------------------------------------------------
// Date
// ---------------------------------------------------------

const DateType &DateType::InstanceNonNullable() {
  static DateType kNonNullableDate(false);
  return kNonNullableDate;
}

const DateType &DateType::InstanceNullable() {
  static DateType kNullableDate(true);
  return kNullableDate;
}

std::string DateType::GetName() const {
  std::string str = "Date";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool DateType::Equals(const SqlType &other) const {
  return other.Is<DateType>() && nullable() == other.nullable();
}

DateType::DateType(bool nullable) : SqlType(SqlTypeId::Date, nullable) {}

// ---------------------------------------------------------
// Fixed-length strings
// ---------------------------------------------------------

template <bool Nullable>
const CharType &CharType::InstanceInternal(u32 length) {
  static llvm::DenseMap<u32, std::unique_ptr<CharType>> kCharTypeMap;

  if (auto iter = kCharTypeMap.find(length); iter != kCharTypeMap.end()) {
    return *iter->second;
  }

  auto iter = kCharTypeMap.try_emplace(length, new CharType(Nullable, length));
  return *iter.first->second;
}

const CharType &CharType::InstanceNonNullable(u32 len) {
  return InstanceInternal<false>(len);
}
const CharType &CharType::InstanceNullable(u32 len) {
  return InstanceInternal<true>(len);
}

CharType::CharType(bool nullable, u32 length)
    : SqlType(SqlTypeId::Char, nullable), length_(length) {}

std::string CharType::GetName() const {
  std::string str = "Char[" + std::to_string(length());
  if (nullable()) {
    str.append(",NULLABLE");
  }
  str.append("]");
  return str;
}

bool CharType::Equals(const SqlType &other) const {
  if (auto *other_char = other.SafeAs<CharType>()) {
    return length() == other_char->length() &&
           nullable() == other_char->nullable();
  }
  return false;
}

u32 CharType::length() const { return length_; }

// ---------------------------------------------------------
// Variable-length strings
// ---------------------------------------------------------

template <bool Nullable>
const VarcharType &VarcharType::InstanceInternal(u32 length) {
  static llvm::DenseMap<u32, std::unique_ptr<VarcharType>> kVarcharTypeMap;

  if (auto iter = kVarcharTypeMap.find(length); iter != kVarcharTypeMap.end()) {
    return *iter->second;
  }

  auto iter =
      kVarcharTypeMap.try_emplace(length, new VarcharType(Nullable, length));
  return *iter.first->second;
}

const VarcharType &VarcharType::InstanceNonNullable(u32 max_len) {
  return InstanceInternal<false>(max_len);
}

const VarcharType &VarcharType::InstanceNullable(u32 max_len) {
  return InstanceInternal<true>(max_len);
}

VarcharType::VarcharType(bool nullable, u32 max_len)
    : SqlType(SqlTypeId::Varchar, nullable), max_len_(max_len) {}

std::string VarcharType::GetName() const {
  std::string str = "Char[" + std::to_string(max_length());
  if (nullable()) {
    str.append(",NULLABLE");
  }
  str.append("]");
  return str;
}

bool VarcharType::Equals(const SqlType &other) const {
  if (auto *other_varchar = other.SafeAs<VarcharType>()) {
    return max_length() == other_varchar->max_length() &&
           nullable() == other_varchar->nullable();
  }
  return false;
}

u32 VarcharType::max_length() const { return max_len_; }

}  // namespace tpl::sql
