#include "sql/data_types.h"

#include <memory>
#include <string>
#include <utility>

#include "llvm/ADT/DenseMap.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Boolean
// ---------------------------------------------------------

BooleanType::BooleanType(bool nullable) : SqlType(SqlTypeId::Boolean, nullable) {}

TypeId BooleanType::GetPrimitiveTypeId() const { return TypeId::Boolean; }

std::string BooleanType::GetName() const {
  std::string str = "Boolean";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool BooleanType::IsArithmetic() const { return false; }

bool BooleanType::Equals(const SqlType &that) const {
  return that.Is<BooleanType>() && nullable() == that.nullable();
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

TinyIntType::TinyIntType(bool nullable) : NumberBaseType(SqlTypeId::TinyInt, nullable) {}

std::string TinyIntType::GetName() const {
  std::string str = "TinyInt";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool TinyIntType::Equals(const SqlType &that) const {
  return that.Is<TinyIntType>() && nullable() == that.nullable();
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

SmallIntType::SmallIntType(bool nullable) : NumberBaseType(SqlTypeId::SmallInt, nullable) {}

std::string SmallIntType::GetName() const {
  std::string str = "SmallInt";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool SmallIntType::Equals(const SqlType &that) const {
  return that.Is<SmallIntType>() && nullable() == that.nullable();
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

IntegerType::IntegerType(bool nullable) : NumberBaseType(SqlTypeId::Integer, nullable) {}

std::string IntegerType::GetName() const {
  std::string str = "Integer";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool IntegerType::Equals(const SqlType &that) const {
  return that.Is<IntegerType>() && nullable() == that.nullable();
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

BigIntType::BigIntType(bool nullable) : NumberBaseType(SqlTypeId::BigInt, nullable) {}

std::string BigIntType::GetName() const {
  std::string str = "BigInt";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool BigIntType::Equals(const SqlType &that) const {
  return that.Is<BigIntType>() && nullable() == that.nullable();
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

bool RealType::Equals(const SqlType &that) const {
  return that.Is<RealType>() && nullable() == that.nullable();
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
// Double
// ---------------------------------------------------------

DoubleType::DoubleType(bool nullable) : NumberBaseType(SqlTypeId::Double, nullable) {}

std::string DoubleType::GetName() const {
  std::string str = "Double";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool DoubleType::Equals(const SqlType &that) const {
  return that.Is<DoubleType>() && nullable() == that.nullable();
}

const DoubleType &DoubleType::InstanceNonNullable() {
  static DoubleType kNonNullableBigInt(false);
  return kNonNullableBigInt;
}

const DoubleType &DoubleType::InstanceNullable() {
  static DoubleType kNullableBigInt(true);
  return kNullableBigInt;
}

// ---------------------------------------------------------
// Decimal
// ---------------------------------------------------------

DecimalType::DecimalType(bool nullable, uint32_t precision, uint32_t scale)
    : SqlType(SqlTypeId::Decimal, nullable), precision_(precision), scale_(scale) {}

TypeId DecimalType::GetPrimitiveTypeId() const { return TypeId::BigInt; }

std::string DecimalType::GetName() const {
  std::string str = "Decimal[" + std::to_string(precision()) + "," + std::to_string(scale());
  if (nullable()) {
    str.append(",NULLABLE");
  }
  str.append("]");
  return str;
}

bool DecimalType::Equals(const SqlType &that) const {
  if (auto *other_decimal = that.SafeAs<DecimalType>()) {
    return precision() == other_decimal->precision() && scale() == other_decimal->scale() &&
           nullable() == that.nullable();
  }
  return false;
}

bool DecimalType::IsArithmetic() const { return true; }

uint32_t DecimalType::precision() const { return precision_; }

uint32_t DecimalType::scale() const { return scale_; }

template <bool Nullable>
const DecimalType &DecimalType::InstanceInternal(uint32_t precision, uint32_t scale) {
  static llvm::DenseMap<std::pair<uint32_t, uint32_t>, std::unique_ptr<DecimalType>>
      kDecimalTypeMap;

  auto key = std::make_pair(precision, scale);
  if (auto iter = kDecimalTypeMap.find(key); iter != kDecimalTypeMap.end()) {
    return *iter->second;
  }

  auto iter = kDecimalTypeMap.try_emplace(key, new DecimalType(Nullable, precision, scale));
  return *iter.first->second;
}

const DecimalType &DecimalType::InstanceNonNullable(uint32_t precision, uint32_t scale) {
  return InstanceInternal<false>(precision, scale);
}

const DecimalType &DecimalType::InstanceNullable(uint32_t precision, uint32_t scale) {
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

TypeId DateType::GetPrimitiveTypeId() const { return TypeId::Date; }

std::string DateType::GetName() const {
  std::string str = "Date";
  if (nullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool DateType::Equals(const SqlType &that) const {
  return that.Is<DateType>() && nullable() == that.nullable();
}

DateType::DateType(bool nullable) : SqlType(SqlTypeId::Date, nullable) {}

// ---------------------------------------------------------
// Fixed-length strings
// ---------------------------------------------------------

template <bool Nullable>
const CharType &CharType::InstanceInternal(uint32_t length) {
  static llvm::DenseMap<uint32_t, std::unique_ptr<CharType>> kCharTypeMap;

  if (auto iter = kCharTypeMap.find(length); iter != kCharTypeMap.end()) {
    return *iter->second;
  }

  auto iter = kCharTypeMap.try_emplace(length, new CharType(Nullable, length));
  return *iter.first->second;
}

const CharType &CharType::InstanceNonNullable(uint32_t len) { return InstanceInternal<false>(len); }
const CharType &CharType::InstanceNullable(uint32_t len) { return InstanceInternal<true>(len); }

CharType::CharType(bool nullable, uint32_t length)
    : SqlType(SqlTypeId::Char, nullable), length_(length) {}

TypeId CharType::GetPrimitiveTypeId() const { return TypeId::Varchar; }

std::string CharType::GetName() const {
  std::string str = "Char[" + std::to_string(length());
  if (nullable()) {
    str.append(",NULLABLE");
  }
  str.append("]");
  return str;
}

bool CharType::Equals(const SqlType &that) const {
  if (auto *other_char = that.SafeAs<CharType>()) {
    return length() == other_char->length() && nullable() == other_char->nullable();
  }
  return false;
}

uint32_t CharType::length() const { return length_; }

// ---------------------------------------------------------
// Variable-length strings
// ---------------------------------------------------------

template <bool Nullable>
const VarcharType &VarcharType::InstanceInternal(uint32_t length) {
  static llvm::DenseMap<uint32_t, std::unique_ptr<VarcharType>> kVarcharTypeMap;

  if (auto iter = kVarcharTypeMap.find(length); iter != kVarcharTypeMap.end()) {
    return *iter->second;
  }

  auto iter = kVarcharTypeMap.try_emplace(length, new VarcharType(Nullable, length));
  return *iter.first->second;
}

const VarcharType &VarcharType::InstanceNonNullable(uint32_t max_len) {
  return InstanceInternal<false>(max_len);
}

const VarcharType &VarcharType::InstanceNullable(uint32_t max_len) {
  return InstanceInternal<true>(max_len);
}

VarcharType::VarcharType(bool nullable, uint32_t max_len)
    : SqlType(SqlTypeId::Varchar, nullable), max_len_(max_len) {}

TypeId VarcharType::GetPrimitiveTypeId() const { return TypeId::Varchar; }

std::string VarcharType::GetName() const {
  std::string str = "Varchar[" + std::to_string(max_length());
  if (nullable()) {
    str.append(",NULLABLE");
  }
  str.append("]");
  return str;
}

bool VarcharType::Equals(const SqlType &that) const {
  if (auto *other_varchar = that.SafeAs<VarcharType>()) {
    return max_length() == other_varchar->max_length() && nullable() == other_varchar->nullable();
  }
  return false;
}

uint32_t VarcharType::max_length() const { return max_len_; }

}  // namespace tpl::sql
