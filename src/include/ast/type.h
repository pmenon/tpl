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

  // Context this type was allocated from
  AstContext &context() const { return ctx_; }

  // Alignment (in bytes) of this type
  std::size_t alignment() const { return align_; }

  // Size (in bytes) of this type
  std::size_t size() const { return size_; }

  // The "kind" of type this is (e.g., Integer, Struct, Array, etc.)
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

  std::string ToString() const { return ToString(this); }

  static std::string ToString(const Type *type);

 protected:
  Type(AstContext &ctx, std::size_t size, std::size_t alignment, Kind kind)
      : ctx_(ctx), size_(size), align_(alignment), kind_(kind) {}

 private:
  AstContext &ctx_;
  std::size_t size_;
  std::size_t align_;
  Kind kind_;
};

/**
 * Integral, fixed width integer type
 */
class IntegerType : public Type {
 public:
  enum class IntKind : uint8_t {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64
  };

  IntKind int_kind() const { return int_kind_; }

  static IntegerType *Int8(AstContext &ctx);
  static IntegerType *Int16(AstContext &ctx);
  static IntegerType *Int32(AstContext &ctx);
  static IntegerType *Int64(AstContext &ctx);
  static IntegerType *UInt8(AstContext &ctx);
  static IntegerType *UInt16(AstContext &ctx);
  static IntegerType *UInt32(AstContext &ctx);
  static IntegerType *UInt64(AstContext &ctx);

  u32 BitWidth() const {
    switch (int_kind()) {
      case IntKind::Int8:
      case IntKind::UInt8: {
        return 8;
      }
      case IntKind::Int16:
      case IntKind::UInt16: {
        return 16;
      }
      case IntKind::Int32:
      case IntKind::UInt32: {
        return 32;
      }
      case IntKind::Int64:
      case IntKind::UInt64: {
        return 64;
      }
    }
  }

  bool IsSigned() const {
    switch (int_kind()) {
      case IntKind::Int8:
      case IntKind::Int16:
      case IntKind::Int32:
      case IntKind::Int64: {
        return true;
      }
      default: { return false; }
    }
  }

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::IntegerType;
  }

 private:
  friend class AstContext;
  IntegerType(AstContext &ctx, std::size_t size, std::size_t alignment,
              IntKind int_kind)
      : Type(ctx, size, alignment, Type::Kind::IntegerType),
        int_kind_(int_kind) {}

 private:
  IntKind int_kind_;
};

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
  FloatType(AstContext &ctx, std::size_t size, std::size_t alignment,
            FloatKind float_kind)
      : Type(ctx, size, alignment, Type::Kind::FloatType),
        float_kind_(float_kind) {}

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
  explicit BoolType(AstContext &ctx)
      : Type(ctx, sizeof(i8), alignof(i8), Type::Kind::BoolType) {}
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
  explicit NilType(AstContext &ctx) : Type(ctx, 0, 0, Type::Kind::NilType) {}
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
      : Type(base->context(), sizeof(i8 *), alignof(i8 *),
             Type::Kind::PointerType),
        base_(base) {}

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
      : Type(elem_type->context(), elem_type->size() * length,
             elem_type->alignment(), Type::Kind::ArrayType),
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
  explicit StructType(AstContext &ctx, std::size_t size, std::size_t alignment,
                      util::RegionVector<Type *> &&fields)
      : Type(ctx, size, alignment, Type::Kind::StructType),
        fields_(std::move(fields)) {}

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
      : Type(ret->context(), 0, 0, Type::Kind::FunctionType),
        params_(std::move(params)),
        ret_(ret) {}

 private:
  util::RegionVector<Type *> params_;
  Type *ret_;
};

}  // namespace tpl::ast