#pragma once

#include <cstdint>

#include "llvm/Support/Casting.h"

#include "util/region.h"
#include "util/region_containers.h"

namespace tpl::ast {

class AstContext;

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
  enum class Kind : uint8_t { TYPE_LIST(F) };
#undef F

  AstContext &context() const { return ctx_; }

  uint32_t alignment() const { return align_; }

  uint32_t width() const { return width_; }

  Kind kind() const { return kind_; }

  template <typename T>
  bool Is() const {
    return llvm::isa<T>(this);
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

#define F(kind) \
  bool Is##kind() const { return Is<kind>(); }
  TYPE_LIST(F)
#undef F

  bool IsNumber() const { return (IsIntegerType() || IsFloatType()); }

  PointerType *PointerTo();

  static std::string GetAsString(Type *type);

 protected:
  Type(AstContext &ctx, Kind kind)
      : ctx_(ctx), align_(0), width_(0), kind_(kind) {}

 private:
  AstContext &ctx_;
  uint32_t align_;
  uint32_t width_;
  Kind kind_;
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
  enum class IntKind : uint8_t { INT_TYPES(F) };
#undef F

  IntKind int_kind() const { return int_kind_; }

#define F(ikind) static IntegerType *ikind(AstContext &ctx);
  INT_TYPES(F)
#undef F

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::IntegerType;
  }

 private:
  friend class AstContext;
  IntegerType(AstContext &ctx, IntKind int_kind)
      : Type(ctx, Type::Kind::IntegerType), int_kind_(int_kind) {}

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

  static FloatType *Float32(AstContext &ctx);

  static FloatType *Float64(AstContext &ctx);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::FloatType;
  }

 private:
  friend class AstContext;
  FloatType(AstContext &ctx, FloatKind float_kind)
      : Type(ctx, Type::Kind::FloatType), float_kind_(float_kind) {}

 private:
  FloatKind float_kind_;
};

/**
 * Boolean type
 */
class BoolType : public Type {
 public:
  static BoolType *Bool(AstContext &ctx);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::BoolType;
  }

 private:
  friend class AstContext;
  explicit BoolType(AstContext &ctx) : Type(ctx, Type::Kind::BoolType) {}
};

/**
 * Nil type
 */
class NilType : public Type {
 public:
  static NilType *Nil(AstContext &ctx);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::NilType;
  }

 private:
  friend class AstContext;
  explicit NilType(AstContext &ctx) : Type(ctx, Type::Kind::NilType) {}
};

/**
 * A pointer type
 */
class PointerType : public Type {
 public:
  Type *base() const { return base_; }

  static PointerType *Get(Type *base);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::PointerType;
  }

 private:
  explicit PointerType(Type *base)
      : Type(base->context(), Type::Kind::PointerType), base_(base) {}

 private:
  Type *base_;
};

/**
 * An array type
 */
class ArrayType : public Type {
 public:
  uint64_t length() const { return length_; }

  Type *element_type() const { return elem_type_; }

  static ArrayType *Get(uint64_t length, Type *elem_type);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::ArrayType;
  }

 private:
  friend class AstContext;
  explicit ArrayType(uint64_t length, Type *elem_type)
      : Type(elem_type->context(), Type::Kind::ArrayType),
        length_(length),
        elem_type_(elem_type) {}

 private:
  uint64_t length_;
  Type *elem_type_;
};

/**
 * A struct type
 */
class StructType : public Type {
 public:
  const util::RegionVector<Type *> &fields() const { return fields_; }

  static StructType *Get(AstContext &ctx, util::RegionVector<Type *> &&fields);
  static StructType *Get(util::RegionVector<Type *> &&fields);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::StructType;
  }

 private:
  explicit StructType(AstContext &ctx, util::RegionVector<Type *> &&fields)
      : Type(ctx, Type::Kind::StructType), fields_(std::move(fields)) {}

 private:
  util::RegionVector<Type *> fields_;
};

/**
 * A function type
 */
class FunctionType : public Type {
 public:
  const util::RegionVector<Type *> &params() const { return params_; }

  Type *return_type() const { return ret_; }

  static FunctionType *Get(util::RegionVector<Type *> &&params, Type *ret);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::FunctionType;
  }

 private:
  friend class AstContext;
  explicit FunctionType(util::RegionVector<Type *> &&params, Type *ret)
      : Type(ret->context(), Type::Kind::FunctionType),
        params_(std::move(params)),
        ret_(ret) {}

 private:
  util::RegionVector<Type *> params_;
  Type *ret_;
};

}  // namespace tpl::ast