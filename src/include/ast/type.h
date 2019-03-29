#pragma once

#include <cstdint>
#include <string>

#include "llvm/Support/Casting.h"

#include "ast/identifier.h"
#include "sql/data_types.h"
#include "util/region.h"
#include "util/region_containers.h"

namespace tpl::ast {

class AstContext;

#define TYPE_LIST(F) \
  F(IntegerType)     \
  F(FloatType)       \
  F(BoolType)        \
  F(StringType)      \
  F(NilType)         \
  F(PointerType)     \
  F(ArrayType)       \
  F(MapType)         \
  F(StructType)      \
  F(FunctionType)    \
  F(InternalType)    \
  F(SqlType)

// Forward declare everything first
#define F(name) class name;
TYPE_LIST(F)
#undef F

class Type : public util::RegionObject {
 public:
#define F(kind) kind,
  enum class Kind : u8 { TYPE_LIST(F) };
#undef F

  // Context this type was allocated from
  AstContext &context() const { return ctx_; }

  // Size (in bytes) of this type
  u32 size() const { return size_; }

  // Alignment (in bytes) of this type
  u32 alignment() const { return align_; }

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

  bool IsArithmetic() const;

  PointerType *PointerTo();

  std::string ToString() const { return ToString(this); }

  static std::string ToString(const Type *type);

 protected:
  Type(AstContext &ctx, u32 size, u32 alignment, Kind kind)  // NOLINT
      : ctx_(ctx), size_(size), align_(alignment), kind_(kind) {}

 private:
  AstContext &ctx_;
  u32 size_;
  u32 align_;
  Kind kind_;
};

/**
 * Integral, fixed width integer type
 */
class IntegerType : public Type {
 public:
  enum class IntKind : u8 {
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

  static IntegerType *Get(const AstContext &ctx, IntKind kind);

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
      default: { UNREACHABLE("Impossible integer kind"); }
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
  // NOLINTNEXTLINE
  IntegerType(AstContext &ctx, u32 size, u32 alignment, IntKind int_kind)
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
  enum class FloatKind : u8 { Float32, Float64 };

  FloatKind float_kind() const { return float_kind_; }

  static FloatType *Get(const AstContext &ctx, FloatKind kind);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::FloatType;
  }

 private:
  friend class AstContext;
  // NOLINTNEXTLINE
  FloatType(AstContext &ctx, u32 size, u32 alignment, FloatKind float_kind)
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
  static BoolType *Get(const AstContext &ctx);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::BoolType;
  }

 private:
  friend class AstContext;
  // NOLINTNEXTLINE
  explicit BoolType(AstContext &ctx)
      : Type(ctx, sizeof(i8), alignof(i8), Type::Kind::BoolType) {}
};

class StringType : public Type {
 public:
  static StringType *Get(const AstContext &ctx);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::StringType;
  }

 private:
  friend class AstContext;
  // NOLINTNEXTLINE
  explicit StringType(AstContext &ctx)
      : Type(ctx, sizeof(i8 *), alignof(i8 *), Type::Kind::StringType) {}
};

/**
 * Nil type
 */
class NilType : public Type {
 public:
  static NilType *Get(const AstContext &ctx);

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
  u64 length() const { return length_; }

  Type *element_type() const { return elem_type_; }

  static ArrayType *Get(u64 length, Type *elem_type);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::ArrayType;
  }

 private:
  explicit ArrayType(u64 length, Type *elem_type)
      : Type(elem_type->context(), elem_type->size() * length,
             elem_type->alignment(), Type::Kind::ArrayType),
        length_(length),
        elem_type_(elem_type) {}

 private:
  u64 length_;
  Type *elem_type_;
};

struct Field {
  Identifier name;
  Type *type;

  Field(const Identifier &name, Type *type) : name(name), type(type) {}

  bool operator==(const Field &other) const noexcept {
    return name == other.name && type == other.type;
  }
};

/**
 * A function type
 */
class FunctionType : public Type {
 public:
  const util::RegionVector<Field> &params() const { return params_; }

  u32 num_params() const { return static_cast<u32>(params().size()); }

  Type *return_type() const { return ret_; }

  static FunctionType *Get(util::RegionVector<Field> &&params, Type *ret);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::FunctionType;
  }

 private:
  explicit FunctionType(util::RegionVector<Field> &&params, Type *ret);

 private:
  util::RegionVector<Field> params_;
  Type *ret_;
};

/**
 * An unordered map (i.e., hashtable)
 */
class MapType : public Type {
 public:
  Type *key_type() const { return key_type_; }

  Type *value_type() const { return val_type_; }

  static MapType *Get(Type *key_type, Type *value_type);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::MapType;
  }

 private:
  MapType(Type *key_type, Type *val_type);

 private:
  Type *key_type_;
  Type *val_type_;
};

/**
 * A struct type
 */
class StructType : public Type {
 public:
  const util::RegionVector<Field> &fields() const { return fields_; }

  Type *LookupFieldByName(Identifier name) const {
    for (const auto &field : fields()) {
      if (field.name == name) {
        return field.type;
      }
    }
    return nullptr;
  }

  u32 GetOffsetOfFieldByName(Identifier name) const {
    for (u32 i = 0; i < fields_.size(); i++) {
      if (fields_[i].name == name) {
        return field_offsets_[i];
      }
    }
    return 0;
  }

  bool IsLayoutIdentical(const StructType &other) const {
    return (this == &other || fields() == other.fields());
  }

  // NOLINTNEXTLINE
  static StructType *Get(AstContext &ctx, util::RegionVector<Field> &&fields);
  static StructType *Get(util::RegionVector<Field> &&fields);

  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::StructType;
  }

 private:
  explicit StructType(AstContext &ctx, u32 size, u32 alignment,  // NOLINT
                      util::RegionVector<Field> &&fields,
                      util::RegionVector<u32> &&field_offsets);

 private:
  util::RegionVector<Field> fields_;
  util::RegionVector<u32> field_offsets_;
};

// This macro lists all internal types. The columns are:
// internal kind name, fully qualified name of class in string, fully qualified
// The LLVM engine uses the names below verbatim to look into the runtime
// for loaded classes. Thus, these names need to be consistent with the FQN
// names of the corresponding classes (i.e., the string form and FQN reference
// should most likely be the same)
// class reference
// clang-format off
#define INTERNAL_TYPE_LIST(V) \
  V(TableVectorIterator, "tpl::sql::TableVectorIterator", ::tpl::sql::TableVectorIterator)                   /* NOLINT */   \
  V(VectorProjectionIterator, "tpl::sql::VectorProjectionIterator", ::tpl::sql::VectorProjectionIterator)    /* NOLINT */   \
  V(JoinHashTable, "tpl::sql::JoinHashTable", ::tpl::sql::JoinHashTable)                                     /* NOLINT */   \
  V(SqlBool, "tpl::sql::Bool", ::tpl::sql::BoolVal)                                                          /* NOLINT */   \
  V(SqlInteger, "tpl::sql::Integer", ::tpl::sql::Integer)                                                    /* NOLINT */   \
  V(SqlDecimal, "tpl::sql::Decimal", ::tpl::sql::Decimal)
// clang-format on

/**
 * Internal types are dedicated to pre-compiled C++ types that we don't want to
 * lift into TPL's type system. While they are usable as regular TPL types, they
 * are not exposed to users (i.e., a user cannot construct one of these types).
 *
 * TODO(pmenon): Is InternalType really a good name for these?
 */
class InternalType : public Type {
 public:
  enum class InternalKind : u8 {
#define DECLARE_TYPE(kind, ...) kind,
    INTERNAL_TYPE_LIST(DECLARE_TYPE)
#undef DECLARE_TYPE
#define COUNT(...) +1
        Last = -1 INTERNAL_TYPE_LIST(COUNT)
#undef COUNT
  };

  static const u32 kNumInternalKinds = static_cast<u32>(InternalKind::Last) + 1;

  const Identifier &name() const { return name_; }

  InternalKind internal_kind() const { return internal_kind_; }

  // Return the number of internal types
  static constexpr u32 NumInternalTypes() { return kNumInternalKinds; }

  // Static factory
  static InternalType *Get(const AstContext &ctx, InternalKind kind);

  // Type check
  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::InternalType;
  }

 private:
  friend class AstContext;
  explicit InternalType(AstContext &ctx, Identifier name, u32 size,  // NOLINT
                        u32 alignment, InternalKind internal_kind)
      : Type(ctx, size, alignment, Type::Kind::InternalType),
        name_(name),
        internal_kind_(internal_kind) {}

 private:
  Identifier name_;
  InternalKind internal_kind_;
};

/**
 * A SQL type masquerading as a TPL type
 */
class SqlType : public Type {
 public:
  // NOLINTNEXTLINE
  static SqlType *Get(AstContext &ctx, const sql::Type &sql_type);

  const sql::Type &sql_type() const { return sql_type_; }

  // Type check
  static bool classof(const Type *type) {
    return type->kind() == Type::Kind::SqlType;
  }

 private:
  // NOLINTNEXTLINE
  SqlType(AstContext &ctx, u32 size, u32 alignment, const sql::Type &sql_type)
      : Type(ctx, size, alignment, Kind::SqlType), sql_type_(sql_type) {}

 private:
  const sql::Type &sql_type_;
};

}  // namespace tpl::ast
