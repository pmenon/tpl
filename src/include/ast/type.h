#pragma once

#include <cstdint>

#include "util/casting.h"
#include "util/region.h"
#include "util/region_containers.h"

namespace tpl::ast {

class AstString;

#define TYPE_LIST(F) \
  F(IntegerType)     \
  F(FloatType)       \
  F(BoolType)        \
  F(NilType)         \
  F(PointerType)     \
  F(ArrayType)       \
  F(StructType)      \
  F(FunctionType)

// Forward declare everything first
#define F(name) class name;
TYPE_LIST(F)
#undef F

class Type : public util::RegionObject {
 public:
#define F(kind) kind,
  enum class Kind : uint8_t { TYPE_LIST(F) LastType };
#undef F

  uint32_t alignment() const { return align_; }

  uint32_t width() const { return width_; }

  Kind kind() const { return kind_; }

  const AstString *name() const { return name_; }

  template <typename T>
  bool Is() const {
    return util::Is<T>(this);
  }

  template <typename T>
  const T *As() const {
    return reinterpret_cast<const T *>(this);
  }

  template <typename T>
  T *As() {
    return reinterpret_cast<T *>(this);
  }

  template <typename T>
  const T *SafeAs() const {
    return (Is<T>() ? As<T>() : nullptr);
  }

  template <typename T>
  T *SafeAs() {
    return (Is<T>() ? As<T>() : nullptr);
  }

#define F(name) \
  bool Is##name() const { return Is<name>(); }
  TYPE_LIST(F)
#undef F

  bool IsNumber() const { return (IsIntegerType() || IsFloatType()); }

 protected:
  Type(Kind kind) : kind_(kind) {}

 private:
  uint32_t align_;
  uint32_t width_;
  Kind kind_;
  AstString *name_;
};

/**
 * Integral, fixed width integer type
 */

#define INT_TYPES(F) \
  F(Int8)            \
  F(UInt8)           \
  F(Int16)           \
  F(UInt16)          \
  F(Int32)           \
  F(UInt32)          \
  F(Int64)           \
  F(UInt64)

class IntegerType : public Type {
 public:
#define F(name) name,
  enum class IntKind : uint8_t { INT_TYPES(F) LastIntType };
#undef F

  IntKind int_kind() const { return int_kind_; }

#define F(name)                                 \
  static IntegerType *name() {                  \
    static IntegerType instance(IntKind::name); \
    return &instance;                           \
  }
  INT_TYPES(F)
#undef F

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::IntegerType;
  }

 private:
  IntegerType(IntKind int_kind)
      : Type(Type::Kind::IntegerType), int_kind_(int_kind) {}

 private:
  IntKind int_kind_;
};

// hygiene
#undef INT_TYPES

/**
 * Floating point number type
 */
class FloatType : public Type {
 public:
  enum class FloatKind : uint8_t { Float32, Float64 };

  FloatKind float_kind() const { return float_kind_; }

  static FloatType *Float32() {
    static FloatType instance(FloatKind::Float32);
    return &instance;
  }

  static FloatType *Float64() {
    static FloatType instance(FloatKind::Float64);
    return &instance;
  }

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::FloatType;
  }

 private:
  FloatType(FloatKind float_kind)
      : Type(Type::Kind::FloatType), float_kind_(float_kind) {}

 private:
  FloatKind float_kind_;
};

/**
 * Boolean type
 */
class BoolType : public Type {
 public:
  static BoolType *True() {
    static BoolType instance;
    return &instance;
  }

  static BoolType *False() {
    static BoolType instance;
    return &instance;
  }

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::BoolType;
  }

 private:
  BoolType() : Type(Type::Kind::BoolType) {}
};

/**
 * Nil type
 */
class NilType : public Type {
 public:
  static NilType *Nil() {
    static NilType instance;
    return &instance;
  }

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::NilType;
  }

 private:
  NilType() : Type(Type::Kind::NilType) {}
};

/**
 * A pointer type
 */
class PointerType : public Type {
 public:
  explicit PointerType(Type *base)
      : Type(Type::Kind::PointerType), base_(base) {}

  Type *base() const { return base_; }

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::PointerType;
  }

 private:
  Type *base_;
};

/**
 * An array type
 */
class ArrayType : public Type {
 public:
  explicit ArrayType(uint64_t length, Type *elem_type)
      : Type(Type::Kind::ArrayType), length_(length), elem_type_(elem_type) {}

  uint64_t length() const { return length_; }

  const Type *element_type() const { return elem_type_; }

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::ArrayType;
  }

 private:
  uint64_t length_;
  Type *elem_type_;
};

/**
 * A struct type
 */
class StructType : public Type {
 public:
  explicit StructType(util::RegionVector<Type *> &&fields)
      : Type(Type::Kind::StructType), fields_(std::move(fields)) {}

  const util::RegionVector<Type *> &fields() const { return fields_; }

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::StructType;
  }

 private:
  util::RegionVector<Type *> fields_;
};

/**
 * A function type
 */
class FunctionType : public Type {
 public:
  explicit FunctionType(util::RegionVector<Type *> &&params, Type *ret)
      : Type(Type::Kind::FunctionType), params_(std::move(params)), ret_(ret) {}

  const util::RegionVector<Type *> &params() const { return params_; }

  Type *return_type() const { return ret_; }

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::FunctionType;
  }

 private:
  util::RegionVector<Type *> params_;
  Type *ret_;
};

}  // namespace tpl::ast