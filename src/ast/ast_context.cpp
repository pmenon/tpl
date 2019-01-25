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

struct AstContext::Implementation {
  static constexpr const uint32_t kDefaultStringTableCapacity = 32;

  //////////////////////////////////////////////////////////
  ///
  /// The basic types
  ///
  //////////////////////////////////////////////////////////

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

  //////////////////////////////////////////////////////////
  ///
  /// Caches
  ///
  //////////////////////////////////////////////////////////

  llvm::StringMap<char, util::LlvmRegionAllocator> string_table;
  llvm::DenseMap<Identifier, Type *> builtin_types;
  llvm::DenseMap<Identifier, Builtin> builtin_funcs;
  llvm::DenseMap<Type *, PointerType *> pointer_types;
  llvm::DenseMap<std::pair<Type *, uint64_t>, ArrayType *> array_types;
  llvm::DenseMap<std::pair<Type *, Type *>, MapType *> map_types;

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

  auto &cached_types = ctx.impl().pointer_types;

  auto iter = cached_types.find(base);
  if (iter != cached_types.end()) {
    return iter->second;
  }

  auto *pointer_type = new (ctx.region()) PointerType(base);

  cached_types.try_emplace(base, pointer_type);

  return pointer_type;
}

// static
ArrayType *ArrayType::Get(uint64_t length, Type *elem_type) {
  AstContext &ctx = elem_type->context();

  auto &cached_types = ctx.impl().array_types;

  auto iter = cached_types.find(std::make_pair(elem_type, length));
  if (iter != cached_types.end()) {
    return iter->second;
  }

  auto *array_type = new (ctx.region()) ArrayType(length, elem_type);

  cached_types.try_emplace(std::make_pair(elem_type, length), array_type);

  return array_type;
}

// static
MapType *MapType::Get(Type *key_type, Type *value_type) {
  AstContext &ctx = key_type->context();

  auto &cached_types = ctx.impl().map_types;

  auto iter = cached_types.find({key_type, value_type});
  if (iter != cached_types.end()) {
    return iter->second;
  }

  auto *map_type = new (ctx.region()) MapType(key_type, value_type);

  cached_types.try_emplace({key_type, value_type}, map_type);

  return map_type;
}

// static
StructType *StructType::Get(AstContext &ctx,
                            util::RegionVector<Field> &&fields) {
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

  return new (ctx.region()) StructType(ctx, size, alignment, std::move(fields),
                                       std::move(field_offsets));
}

// static
StructType *StructType::Get(util::RegionVector<Field> &&fields) {
  TPL_ASSERT(!fields.empty(),
             "Cannot use StructType::Get(fields) with an empty list of fields");
  return StructType::Get(fields[0].type->context(), std::move(fields));
}

// static
FunctionType *FunctionType::Get(util::RegionVector<Field> &&params, Type *ret) {
  // TODO(pmenon): Use cache
  return new (ret->context().region()) FunctionType(std::move(params), ret);
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