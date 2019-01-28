#include "ast/ast_context.h"

#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/StringMap.h"

#include "ast/ast_node_factory.h"
#include "ast/builtins.h"
#include "ast/type.h"
#include "sql/table_vector_iterator.h"
#include "sql/value.h"
#include "util/common.h"
#include "util/math_util.h"

namespace tpl::ast {

// ---------------------------------------------------------
// Key type used in the cache for struct types in the context
// ---------------------------------------------------------

struct StructTypeKeyInfo {
  struct KeyTy {
    llvm::SmallVector<Type *, 8> elements;

    explicit KeyTy(const util::RegionVector<Field> &elems) noexcept {
      elements.resize(elems.size());
      for (u32 i = 0; i < elems.size(); i++) {
        elements[i] = elems[i].type;
      }
    }

    explicit KeyTy(const StructType *struct_type) {
      elements.resize(struct_type->fields().size());
      for (u32 i = 0; i < struct_type->fields().size(); i++) {
        elements[i] = struct_type->fields()[i].type;
      }
    }

    bool operator==(const KeyTy &that) const {
      return elements == that.elements;
    }

    bool operator!=(const KeyTy &that) const { return !this->operator==(that); }
  };

  static inline StructType *getEmptyKey() {
    return llvm::DenseMapInfo<StructType *>::getEmptyKey();
  }

  static inline StructType *getTombstoneKey() {
    return llvm::DenseMapInfo<StructType *>::getTombstoneKey();
  }

  static std::size_t getHashValue(const KeyTy &key) {
    return llvm::hash_combine_range(key.elements.begin(), key.elements.end());
  }

  static std::size_t getHashValue(const StructType *struct_type) {
    return getHashValue(KeyTy(struct_type));
  }

  static bool isEqual(const KeyTy &lhs, const StructType *rhs) {
    if (rhs == getEmptyKey() || rhs == getTombstoneKey()) return false;
    return lhs == KeyTy(rhs);
  }

  static bool isEqual(const StructType *lhs, const StructType *rhs) {
    return lhs == rhs;
  }
};

// ---------------------------------------------------------
// Key type used in the cache for function types in the context
// ---------------------------------------------------------

struct FunctionTypeKeyInfo {
  struct KeyTy {
    Type *ret_type;
    llvm::SmallVector<Type *, 8> params;

    explicit KeyTy(Type *ret_type, const util::RegionVector<Field> &ps)
        : ret_type(ret_type) {
      params.resize(ps.size());
      for (u32 i = 0; i < ps.size(); i++) {
        params[i] = ps[i].type;
      }
    }

    explicit KeyTy(const FunctionType *func_type) {
      ret_type = func_type->return_type();
      params.resize(func_type->params().size());
      for (u32 i = 0; i < func_type->params().size(); i++) {
        params[i] = func_type->params()[i].type;
      }
    }

    bool operator==(const KeyTy &that) const { return params == that.params; }

    bool operator!=(const KeyTy &that) const { return !this->operator==(that); }
  };

  static inline FunctionType *getEmptyKey() {
    return llvm::DenseMapInfo<FunctionType *>::getEmptyKey();
  }

  static inline FunctionType *getTombstoneKey() {
    return llvm::DenseMapInfo<FunctionType *>::getTombstoneKey();
  }

  static std::size_t getHashValue(const KeyTy &key) {
    return llvm::hash_combine(
        key.ret_type,
        llvm::hash_combine_range(key.params.begin(), key.params.end()));
  }

  static std::size_t getHashValue(const FunctionType *func_type) {
    return getHashValue(KeyTy(func_type));
  }

  static bool isEqual(const KeyTy &lhs, const FunctionType *rhs) {
    if (rhs == getEmptyKey() || rhs == getTombstoneKey()) return false;
    return lhs == KeyTy(rhs);
  }

  static bool isEqual(const FunctionType *lhs, const FunctionType *rhs) {
    return lhs == rhs;
  }
};

struct AstContext::Implementation {
  static constexpr const uint32_t kDefaultStringTableCapacity = 32;

  // -------------------------------------------------------
  // Basic primitive types
  // -------------------------------------------------------

  IntegerType int8;
  IntegerType int16;
  IntegerType int32;
  IntegerType int64;
  IntegerType uint8;
  IntegerType uint16;
  IntegerType uint32;
  IntegerType uint64;

  FloatType float32;
  FloatType float64;

  BoolType boolean;

  StringType string;

  NilType nil;

  util::RegionVector<InternalType *> internal_types;

  // -------------------------------------------------------
  // Complex type caches
  // -------------------------------------------------------

  llvm::StringMap<char, util::LlvmRegionAllocator> string_table;
  llvm::DenseMap<Identifier, Type *> builtin_types;
  llvm::DenseMap<Identifier, Builtin> builtin_funcs;
  llvm::DenseMap<Type *, PointerType *> pointer_types;
  llvm::DenseMap<std::pair<Type *, uint64_t>, ArrayType *> array_types;
  llvm::DenseMap<std::pair<Type *, Type *>, MapType *> map_types;
  llvm::DenseSet<StructType *, StructTypeKeyInfo> struct_types;
  llvm::DenseSet<FunctionType *, FunctionTypeKeyInfo> func_types;

  explicit Implementation(AstContext &ctx)
      : int8(ctx, sizeof(i8), alignof(i8), IntegerType::IntKind::Int8),
        int16(ctx, sizeof(i16), alignof(i16), IntegerType::IntKind::Int16),
        int32(ctx, sizeof(i32), alignof(i32), IntegerType::IntKind::Int32),
        int64(ctx, sizeof(i64), alignof(i64), IntegerType::IntKind::Int64),
        uint8(ctx, sizeof(u8), alignof(u8), IntegerType::IntKind::UInt8),
        uint16(ctx, sizeof(u16), alignof(u16), IntegerType::IntKind::UInt16),
        uint32(ctx, sizeof(u32), alignof(u32), IntegerType::IntKind::UInt32),
        uint64(ctx, sizeof(u64), alignof(u64), IntegerType::IntKind::UInt64),
        float32(ctx, sizeof(f32), alignof(f32), FloatType::FloatKind::Float32),
        float64(ctx, sizeof(f64), alignof(f64), FloatType::FloatKind::Float64),
        boolean(ctx),
        string(ctx),
        nil(ctx),
        internal_types(ctx.region()),
        string_table(kDefaultStringTableCapacity,
                     util::LlvmRegionAllocator(ctx.region())) {}
};

AstContext::AstContext(util::Region *region,
                       sema::ErrorReporter &error_reporter)
    : region_(region),
      error_reporter_(error_reporter),
      node_factory_(std::make_unique<AstNodeFactory>(region)),
      impl_(std::make_unique<Implementation>(*this)) {
  // Initialize basic types
  impl().builtin_types[GetIdentifier("bool")] = &impl().boolean;
  impl().builtin_types[GetIdentifier("nil")] = &impl().nil;
  impl().builtin_types[GetIdentifier("int8")] = &impl().int8;
  impl().builtin_types[GetIdentifier("int16")] = &impl().int16;
  impl().builtin_types[GetIdentifier("int32")] = &impl().int32;
  impl().builtin_types[GetIdentifier("int64")] = &impl().int64;
  impl().builtin_types[GetIdentifier("uint8")] = &impl().uint8;
  impl().builtin_types[GetIdentifier("uint16")] = &impl().uint16;
  impl().builtin_types[GetIdentifier("uint32")] = &impl().uint32;
  impl().builtin_types[GetIdentifier("uint64")] = &impl().uint64;
  impl().builtin_types[GetIdentifier("float32")] = &impl().float32;
  impl().builtin_types[GetIdentifier("float64")] = &impl().float64;
  // Aliases
  impl().builtin_types[GetIdentifier("int")] = &impl().int32;
  impl().builtin_types[GetIdentifier("float")] = &impl().float32;
  impl().builtin_types[GetIdentifier("void")] = &impl().nil;

  // Populate all the internal/hidden/opaque types
  impl().internal_types.reserve(InternalType::NumInternalTypes());
#define INIT_TYPE(Kind, NameStr, Type)                            \
  impl().internal_types.push_back(new (region) InternalType(      \
      *this, GetIdentifier(NameStr), sizeof(Type), alignof(Type), \
      InternalType::InternalKind::Kind));
  INTERNAL_TYPE_LIST(INIT_TYPE)
#undef INIT_TYPE

  // Initialize builtin functions
#define BUILTIN_FUNC(Name, ...)       \
  impl().builtin_funcs[GetIdentifier( \
      Builtins::GetFunctionName(Builtin::Name))] = Builtin::Name;
  BUILTINS_LIST(BUILTIN_FUNC)
#undef BUILTIN_FUNC
}

AstContext::~AstContext() = default;

Identifier AstContext::GetIdentifier(llvm::StringRef str) {
  if (str.empty()) return Identifier(nullptr);

  auto iter = impl().string_table.insert(std::make_pair(str, char(0))).first;
  return Identifier(iter->getKeyData());
}

Type *AstContext::LookupBuiltinType(Identifier identifier) const {
  auto iter = impl().builtin_types.find(identifier);
  return (iter == impl().builtin_types.end() ? nullptr : iter->second);
}

bool AstContext::IsBuiltinFunction(Identifier identifier,
                                   Builtin *builtin) const {
  if (auto iter = impl().builtin_funcs.find(identifier);
      iter != impl().builtin_funcs.end()) {
    if (builtin != nullptr) {
      *builtin = iter->second;
    }
    return true;
  }

  return false;
}

// static
PointerType *Type::PointerTo() { return PointerType::Get(this); }

// static
IntegerType *IntegerType::Get(AstContext &ctx, IntegerType::IntKind int_kind) {
  switch (int_kind) {
    case IntegerType::IntKind::Int8: {
      return &ctx.impl().int8;
    }
    case IntegerType::IntKind::Int16: {
      return &ctx.impl().int16;
    }
    case IntegerType::IntKind::Int32: {
      return &ctx.impl().int32;
    }
    case IntegerType::IntKind::Int64: {
      return &ctx.impl().int64;
    }
    case IntegerType::IntKind::UInt8: {
      return &ctx.impl().uint8;
    }
    case IntegerType::IntKind::UInt16: {
      return &ctx.impl().uint16;
    }
    case IntegerType::IntKind::UInt32: {
      return &ctx.impl().uint32;
    }
    case IntegerType::IntKind::UInt64: {
      return &ctx.impl().uint64;
    }
    default: { UNREACHABLE("Impossible integer kind"); }
  }
}

// static
FloatType *FloatType::Get(AstContext &ctx, FloatKind float_kind) {
  switch (float_kind) {
    case FloatType::FloatKind::Float32: {
      return &ctx.impl().float32;
    }
    case FloatType::FloatKind::Float64: {
      return &ctx.impl().float64;
    }
    default: { UNREACHABLE("Impossible floating point kind"); }
  }
}

// static
BoolType *BoolType::Get(AstContext &ctx) { return &ctx.impl().boolean; }

// static
StringType *StringType::Get(AstContext &ctx) { return &ctx.impl().string; }

// static
NilType *NilType::Get(AstContext &ctx) { return &ctx.impl().nil; }

// static
PointerType *PointerType::Get(Type *base) {
  AstContext &ctx = base->context();

  PointerType *&pointer_type = ctx.impl().pointer_types[base];

  if (pointer_type == nullptr) {
    pointer_type = new (ctx.region()) PointerType(base);
  }

  return pointer_type;
}

// static
ArrayType *ArrayType::Get(uint64_t length, Type *elem_type) {
  AstContext &ctx = elem_type->context();

  ArrayType *&array_type = ctx.impl().array_types[{elem_type, length}];

  if (array_type == nullptr) {
    array_type = new (ctx.region()) ArrayType(length, elem_type);
  }

  return array_type;
}

// static
MapType *MapType::Get(Type *key_type, Type *value_type) {
  AstContext &ctx = key_type->context();

  MapType *&map_type = ctx.impl().map_types[{key_type, value_type}];

  if (map_type == nullptr) {
    map_type = new (ctx.region()) MapType(key_type, value_type);
  }

  return map_type;
}

// static
StructType *StructType::Get(AstContext &ctx,
                            util::RegionVector<Field> &&fields) {
  const StructTypeKeyInfo::KeyTy key(fields);

  auto [iter, inserted] = ctx.impl().struct_types.insert_as(nullptr, key);

  StructType *struct_type = nullptr;

  if (inserted) {
    // Compute size and alignment. Alignment of struct is alignment of largest
    // struct element.
    u32 size = 0;
    u32 alignment = 0;
    util::RegionVector<u32> field_offsets(ctx.region());
    for (const auto &field : fields) {
      // Check if the type needs to be padded
      u32 field_align = field.type->alignment();
      if (!util::MathUtil::IsAligned(size, field_align)) {
        size = static_cast<u32>(util::MathUtil::AlignTo(size, field_align));
      }

      // Update size and calculate alignment
      field_offsets.push_back(size);
      size += field.type->size();
      alignment = std::max(alignment, field.type->alignment());
    }

    struct_type = new (ctx.region()) StructType(
        ctx, size, alignment, std::move(fields), std::move(field_offsets));
    *iter = struct_type;
  } else {
    struct_type = *iter;
  }

  return struct_type;
}

// static
StructType *StructType::Get(util::RegionVector<Field> &&fields) {
  TPL_ASSERT(!fields.empty(),
             "Cannot use StructType::Get(fields) with an empty list of fields");
  return StructType::Get(fields[0].type->context(), std::move(fields));
}

// static
FunctionType *FunctionType::Get(util::RegionVector<Field> &&params, Type *ret) {
  AstContext &ctx = ret->context();

  const FunctionTypeKeyInfo::KeyTy key(ret, params);

  auto [iter, inserted] = ctx.impl().func_types.insert_as(nullptr, key);

  FunctionType *func_type = nullptr;

  if (inserted) {
    // The function type was not in the cache, create the type now and insert it
    // into the cache
    func_type = new (ctx.region()) FunctionType(std::move(params), ret);
    *iter = func_type;
  } else {
    func_type = *iter;
  }

  return func_type;
}

// static
InternalType *InternalType::Get(AstContext &ctx, InternalKind kind) {
  TPL_ASSERT(static_cast<u8>(kind) < kNumInternalKinds,
             "Invalid internal kind");
  return ctx.impl().internal_types[static_cast<u32>(kind)];
}

SqlType *SqlType::Get(AstContext &ctx, const sql::Type &sql_type) {
  // TODO: cache
  u32 size = 0, alignment = 0;
  switch (sql_type.type_id()) {
    case sql::TypeId::Boolean:
    case sql::TypeId::SmallInt:
    case sql::TypeId::Integer:
    case sql::TypeId::Date:
    case sql::TypeId::BigInt: {
      size = sizeof(sql::Integer);
      alignment = alignof(sql::Integer);
      break;
    }
    case sql::TypeId::Decimal: {
      size = sizeof(sql::Decimal);
      alignment = alignof(sql::Decimal);
      break;
    }
    case sql::TypeId::Char:
    case sql::TypeId::Varchar: {
      size = sizeof(sql::String);
      alignment = sizeof(sql::String);
      break;
    }
  }

  return new (ctx.region()) SqlType(ctx, size, alignment, sql_type);
}

}  // namespace tpl::ast