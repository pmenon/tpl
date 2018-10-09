#include "ast/ast_context.h"

#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/StringMap.h"

#include "ast/ast_node_factory.h"
#include "ast/type.h"
#include "runtime/scanner.h"
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

  NilType nil;

  util::RegionVector<InternalType *> internal_types;

  //////////////////////////////////////////////////////////
  ///
  /// Caches
  ///
  //////////////////////////////////////////////////////////

  llvm::StringMap<char, util::LlvmRegionAllocator> string_table;
  llvm::DenseMap<ast::Identifier, ast::Type *> builtin_types;
  llvm::DenseMap<Type *, PointerType *> pointer_types;
  llvm::DenseMap<std::pair<Type *, uint64_t>, ArrayType *> array_types;

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
        nil(ctx),
        internal_types(ctx.region()),
        string_table(kDefaultStringTableCapacity,
                     util::LlvmRegionAllocator(ctx.region())) {}
};

AstContext::AstContext(util::Region &region,
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
}

AstContext::~AstContext() = default;

Identifier AstContext::GetIdentifier(llvm::StringRef str) {
  if (str.empty()) return Identifier(nullptr);

  auto iter = impl().string_table.insert(std::make_pair(str, char(0))).first;
  return Identifier(iter->getKeyData());
}

ast::Type *AstContext::LookupBuiltin(Identifier identifier) {
  auto iter = impl().builtin_types.find(identifier);
  return (iter == impl().builtin_types.end() ? nullptr : iter->second);
}

// static
PointerType *Type::PointerTo() { return PointerType::Get(this); }

// static
IntegerType *IntegerType::Int8(AstContext &ctx) { return &ctx.impl().int8; }

// static
IntegerType *IntegerType::UInt8(AstContext &ctx) { return &ctx.impl().uint8; }

// static
IntegerType *IntegerType::Int16(AstContext &ctx) { return &ctx.impl().int16; }

// static
IntegerType *IntegerType::UInt16(AstContext &ctx) { return &ctx.impl().uint16; }

// static
IntegerType *IntegerType::Int32(AstContext &ctx) { return &ctx.impl().int32; }

// static
IntegerType *IntegerType::UInt32(AstContext &ctx) { return &ctx.impl().uint32; }

// static
IntegerType *IntegerType::Int64(AstContext &ctx) { return &ctx.impl().int64; }

// static
IntegerType *IntegerType::UInt64(AstContext &ctx) { return &ctx.impl().uint64; }

// static
FloatType *FloatType::Float32(tpl::ast::AstContext &ctx) {
  return &ctx.impl().float32;
}

// static
FloatType *FloatType::Float64(tpl::ast::AstContext &ctx) {
  return &ctx.impl().float64;
}

// static
BoolType *BoolType::Bool(AstContext &ctx) { return &ctx.impl().boolean; }

// static
NilType *NilType::Nil(AstContext &ctx) { return &ctx.impl().nil; }

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
StructType *StructType::Get(AstContext &ctx,
                            util::RegionVector<Type *> &&fields) {
  // TODO(pmenon): Use cache?

  // Compute size and alignment. Alignment of struct is alignment of largest
  // struct element.
  std::size_t size = 0;
  std::size_t alignment = 0;
  for (const auto *type : fields) {
    // Check if the type needs to be padded
    if (!util::MathUtil::IsAligned(size, type->alignment())) {
      size = util::MathUtil::AlignTo(size, type->alignment());
    }

    // Update size and calculate alignment
    size += type->size();
    alignment = std::max(alignment, type->alignment());
  }

  // Done
  return new (ctx.region()) StructType(ctx, size, alignment, std::move(fields));
}

// static
StructType *StructType::Get(util::RegionVector<Type *> &&fields) {
  TPL_ASSERT(!fields.empty(),
             "Cannot use StructType::Get(fields) with an empty list of fields");
  return StructType::Get(fields[0]->context(), std::move(fields));
}

// static
FunctionType *FunctionType::Get(util::RegionVector<Type *> &&params,
                                Type *ret) {
  // TODO(pmenon): Use cache
  return new (ret->context().region()) FunctionType(std::move(params), ret);
}

// static
InternalType *InternalType::Get(AstContext &ctx, InternalKind kind) {
  TPL_ASSERT(kind < InternalType::InternalKind::Last, "Invalid internal kind");
  return ctx.impl().internal_types[static_cast<u32>(kind)];
}

}  // namespace tpl::ast