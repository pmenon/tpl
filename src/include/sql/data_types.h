#pragma once

#include <string>

#include "llvm/Support/Casting.h"

#include "sql/sql.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql {

/**
 * Base class for algebraic SQL types.
 */
class SqlType {
 public:
  virtual ~SqlType() = default;

  SqlTypeId id() const { return id_; }

  bool nullable() const { return nullable_; }

  virtual const SqlType &GetNonNullableVersion() const = 0;

  virtual const SqlType &GetNullableVersion() const = 0;

  virtual TypeId GetPrimitiveTypeId() const = 0;

  virtual std::string GetName() const = 0;

  virtual bool IsArithmetic() const = 0;

  virtual bool Equals(const SqlType &other) const = 0;

  // -------------------------------------------------------
  // Type-checking
  // -------------------------------------------------------

  template <typename T>
  bool Is() const {
    return llvm::isa<T>(this);
  }

  template <typename T>
  T *As() {
    return llvm::cast<T>(this);
  }

  template <typename T>
  const T *As() const {
    return llvm::cast<const T>(this);
  }

  template <typename T>
  T *SafeAs() {
    return llvm::dyn_cast<T>(this);
  }

  template <typename T>
  const T *SafeAs() const {
    return llvm::dyn_cast<const T>(this);
  }

 protected:
  explicit SqlType(SqlTypeId sql_type_id, bool nullable) : id_(sql_type_id), nullable_(nullable) {}

 private:
  // The SQL type ID
  SqlTypeId id_;
  // Flag indicating if the type is nullable
  bool nullable_;
};

/**
 * A SQL boolean type.
 */
class BooleanType : public SqlType {
 public:
  static const BooleanType &InstanceNonNullable();

  static const BooleanType &InstanceNullable();

  static const BooleanType &Instance(bool nullable) {
    return (nullable ? InstanceNullable() : InstanceNonNullable());
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool IsArithmetic() const override;

  bool Equals(const SqlType &other) const override;

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::Boolean; }

 private:
  explicit BooleanType(bool nullable);
};

/**
 * Base for all types that are stored as primitive C/C++ numbers. This includes
 * the regular SQL numbers (smallint, int, decimal), but also dates and
 * timestamps.
 * @tparam CppType The primitive type
 */
template <typename CppType>
class NumberBaseType : public SqlType {
 public:
  TypeId GetPrimitiveTypeId() const override { return GetTypeId<CppType>(); }

  bool IsArithmetic() const override { return true; }

 protected:
  NumberBaseType(SqlTypeId type_id, bool nullable) : SqlType(type_id, nullable) {}
};

/**
 * A SQL tiny-int type..
 */
class TinyIntType : public NumberBaseType<i8> {
 public:
  static const TinyIntType &InstanceNonNullable();

  static const TinyIntType &InstanceNullable();

  static const TinyIntType &Instance(bool nullable) {
    return (nullable ? InstanceNullable() : InstanceNonNullable());
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &other) const override;

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::TinyInt; }

 private:
  explicit TinyIntType(bool nullable);
};

/**
 * A SQL small-int type..
 */
class SmallIntType : public NumberBaseType<i16> {
 public:
  static const SmallIntType &InstanceNonNullable();

  static const SmallIntType &InstanceNullable();

  static const SmallIntType &Instance(bool nullable) {
    return (nullable ? InstanceNullable() : InstanceNonNullable());
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &other) const override;

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::SmallInt; }

 private:
  explicit SmallIntType(bool nullable);
};

/**
 * A SQL integer type.
 */
class IntegerType : public NumberBaseType<i32> {
 public:
  static const IntegerType &InstanceNonNullable();

  static const IntegerType &InstanceNullable();

  static const IntegerType &Instance(bool nullable) {
    return (nullable ? InstanceNullable() : InstanceNonNullable());
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &other) const override;

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::Integer; }

 private:
  explicit IntegerType(bool nullable);
};

/**
 * A SQL bigint type.
 */
class BigIntType : public NumberBaseType<i64> {
 public:
  static const BigIntType &InstanceNonNullable();

  static const BigIntType &InstanceNullable();

  static const BigIntType &Instance(bool nullable) {
    return (nullable ? InstanceNullable() : InstanceNonNullable());
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &other) const override;

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::BigInt; }

 private:
  explicit BigIntType(bool nullable);
};

/**
 * A SQL real type, i.e., a 4-byte floating point type.
 */
class RealType : public NumberBaseType<f32> {
 public:
  static const RealType &InstanceNonNullable();

  static const RealType &InstanceNullable();

  static const RealType &Instance(bool nullable) {
    return (nullable ? InstanceNullable() : InstanceNonNullable());
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &other) const override;

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::Real; }

 private:
  explicit RealType(bool nullable);
};

/**
 * A SQL double type, i.e., an 8-byte floating point type.
 */
class DoubleType : public NumberBaseType<f64> {
 public:
  static const DoubleType &InstanceNonNullable();

  static const DoubleType &InstanceNullable();

  static const DoubleType &Instance(bool nullable) {
    return (nullable ? InstanceNullable() : InstanceNonNullable());
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &other) const override;

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::Double; }

 private:
  explicit DoubleType(bool nullable);
};

/**
 * A SQL decimal type.
 */
class DecimalType : public SqlType {
 public:
  static const DecimalType &InstanceNonNullable(u32 precision, u32 scale);

  static const DecimalType &InstanceNullable(u32 precision, u32 scale);

  static const DecimalType &Instance(bool nullable, u32 precision, u32 scale) {
    return (nullable ? InstanceNullable(precision, scale) : InstanceNonNullable(precision, scale));
  }

  const SqlType &GetNonNullableVersion() const override {
    return InstanceNonNullable(precision(), scale());
  }

  const SqlType &GetNullableVersion() const override {
    return InstanceNullable(precision(), scale());
  }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool Equals(const SqlType &other) const override;

  bool IsArithmetic() const override;

  u32 precision() const;

  u32 scale() const;

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::Decimal; }

 private:
  DecimalType(bool nullable, u32 precision, u32 scale);

  template <bool nullable>
  static const DecimalType &InstanceInternal(u32 precision, u32 scale);

 private:
  u32 precision_;
  u32 scale_;
};

/**
 * A SQL date type.
 */
class DateType : public SqlType {
 public:
  static const DateType &InstanceNonNullable();

  static const DateType &InstanceNullable();

  static const DateType &Instance(bool nullable) {
    return (nullable ? InstanceNullable() : InstanceNonNullable());
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool Equals(const SqlType &other) const override;

  bool IsArithmetic() const override { return false; }

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::Date; }

 private:
  explicit DateType(bool nullable);
};

/**
 * A SQL char type.
 */
class CharType : public SqlType {
 public:
  static const CharType &InstanceNonNullable(u32 len);

  static const CharType &InstanceNullable(u32 len);

  static const CharType &Instance(bool nullable, u32 len) {
    return (nullable ? InstanceNullable(len) : InstanceNonNullable(len));
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(length()); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(length()); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool Equals(const SqlType &other) const override;

  bool IsArithmetic() const override { return false; }

  u32 length() const;

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::Char; }

 private:
  explicit CharType(bool nullable, u32 length);

  template <bool nullable>
  static const CharType &InstanceInternal(u32 length);

 private:
  u32 length_;
};

/**
 * A SQL varchar type.
 */
class VarcharType : public SqlType {
 public:
  static const VarcharType &InstanceNonNullable(u32 max_len);

  static const VarcharType &InstanceNullable(u32 max_len);

  static const VarcharType &Instance(bool nullable, u32 max_len) {
    return (nullable ? InstanceNullable(max_len) : InstanceNonNullable(max_len));
  }

  const SqlType &GetNonNullableVersion() const override {
    return InstanceNonNullable(max_length());
  }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(max_length()); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool Equals(const SqlType &other) const override;

  bool IsArithmetic() const override { return false; }

  u32 max_length() const;

  static bool classof(const SqlType *type) { return type->id() == SqlTypeId::Varchar; }

 private:
  explicit VarcharType(bool nullable, u32 max_len);

  template <bool nullable>
  static const VarcharType &InstanceInternal(u32 length);

 private:
  u32 max_len_;
};

}  // namespace tpl::sql
