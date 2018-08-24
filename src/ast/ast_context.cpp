#include "ast/ast_context.h"

#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/StringMap.h"

#include "ast/ast_node_factory.h"
#include "ast/type.h"

namespace tpl::ast {

struct AstContext::Implementation {
  static constexpr const uint32_t kDefaultStringTableCapacity = 32;

  //////////////////////////////////////////////////////////
  ///
  /// The basic types
  ///
  //////////////////////////////////////////////////////////

  IntegerType int8;
  IntegerType uint8;
  IntegerType int16;
  IntegerType uint16;
  IntegerType int32;
  IntegerType uint32;
  IntegerType int64;
  IntegerType uint64;

  FloatType float32;
  FloatType float64;

  BoolType boolean;

  NilType nil;

  //////////////////////////////////////////////////////////
  ///
  /// Caches
  ///
  //////////////////////////////////////////////////////////

  llvm::StringMap<char, util::LlvmRegionAllocator> string_table_;
  llvm::DenseMap<ast::Identifier, ast::Type *> builtin_types_;
  llvm::DenseMap<Type *, PointerType *> pointer_types_;
  llvm::DenseMap<std::pair<Type *, uint64_t>, ArrayType *> array_types_;

  explicit Implementation(AstContext &ctx)
      : int8(ctx, IntegerType::IntKind::Int8),
        uint8(ctx, IntegerType::IntKind::UInt8),
        int16(ctx, IntegerType::IntKind::Int16),
        uint16(ctx, IntegerType::IntKind::UInt16),
        int32(ctx, IntegerType::IntKind::Int32),
        uint32(ctx, IntegerType::IntKind::UInt32),
        int64(ctx, IntegerType::IntKind::Int64),
        uint64(ctx, IntegerType::IntKind::UInt64),
        float32(ctx, FloatType::FloatKind::Float32),
        float64(ctx, FloatType::FloatKind::Float64),
        boolean(ctx),
        nil(ctx),
        string_table_(kDefaultStringTableCapacity,
                      util::LlvmRegionAllocator(ctx.region())) {}
};

AstContext::AstContext(util::Region &region,
                       sema::ErrorReporter &error_reporter)
    : region_(region),
      node_factory_(std::make_unique<AstNodeFactory>(region)),
      error_reporter_(error_reporter),
      impl_(std::make_unique<Implementation>(*this)) {
  // Initialize builtin types
  // clang-format off
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("bool"), &impl().boolean));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("nil"), &impl().nil));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("int8"), &impl().int8));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("int16"), &impl().int16));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("int32"), &impl().int32));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("int64"), &impl().int64));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("uint8"), &impl().uint8));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("uint16"), &impl().uint16));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("uint32"), &impl().uint32));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("uint64"), &impl().uint64));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("float32"), &impl().float32));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("float64"), &impl().float64));
  // Typedefs
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("int"), &impl().int32));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("float"), &impl().float32));
  impl().builtin_types_.insert(std::make_pair(GetIdentifier("void"), &impl().nil));
  // clang-format on
}

AstContext::~AstContext() = default;

Identifier AstContext::GetIdentifier(llvm::StringRef str) {
  if (str.empty()) return Identifier(nullptr);

  auto iter = impl().string_table_.insert(std::make_pair(str, char(0))).first;
  return Identifier(iter->getKeyData());
}

ast::Type *AstContext::LookupBuiltin(Identifier identifier) {
  auto iter = impl().builtin_types_.find(identifier);
  return (iter == impl().builtin_types_.end() ? nullptr : iter->second);
}

PointerType *Type::PointerTo() { return PointerType::Get(this); }

IntegerType *IntegerType::Int8(AstContext &ctx) { return &ctx.impl().int8; }

IntegerType *IntegerType::UInt8(AstContext &ctx) { return &ctx.impl().uint8; }

IntegerType *IntegerType::Int16(AstContext &ctx) { return &ctx.impl().int16; }

IntegerType *IntegerType::UInt16(AstContext &ctx) { return &ctx.impl().uint16; }

IntegerType *IntegerType::Int32(AstContext &ctx) { return &ctx.impl().int32; }

IntegerType *IntegerType::UInt32(AstContext &ctx) { return &ctx.impl().uint32; }

IntegerType *IntegerType::Int64(AstContext &ctx) { return &ctx.impl().int64; }

IntegerType *IntegerType::UInt64(AstContext &ctx) { return &ctx.impl().uint64; }

FloatType *FloatType::Float32(tpl::ast::AstContext &ctx) {
  return &ctx.impl().float32;
}

FloatType *FloatType::Float64(tpl::ast::AstContext &ctx) {
  return &ctx.impl().float64;
}

BoolType *BoolType::Bool(AstContext &ctx) { return &ctx.impl().boolean; }

NilType *NilType::Nil(AstContext &ctx) { return &ctx.impl().nil; }

PointerType *PointerType::Get(Type *base) {
  AstContext &ctx = base->context();

  auto &cached_types = ctx.impl().pointer_types_;

  auto iter = cached_types.find(base);
  if (iter != cached_types.end()) {
    return iter->second;
  }

  auto *pointer_type = new (ctx.region()) PointerType(base);

  cached_types.try_emplace(base, pointer_type);

  return pointer_type;
}

ArrayType *ArrayType::Get(uint64_t length, Type *elem_type) {
  AstContext &ctx = elem_type->context();

  auto &cached_types = ctx.impl().array_types_;

  auto iter = cached_types.find(std::make_pair(elem_type, length));
  if (iter != cached_types.end()) {
    return iter->second;
  }

  auto *array_type = new (ctx.region()) ArrayType(length, elem_type);

  cached_types.try_emplace(std::make_pair(elem_type, length), array_type);

  return array_type;
}

StructType *StructType::Get(AstContext &ctx,
                            util::RegionVector<Type *> &&fields) {
  // TODO(pmenon): Use cache?
  return new (ctx.region()) StructType(ctx, std::move(fields));
}

StructType *StructType::Get(util::RegionVector<Type *> &&fields) {
  TPL_ASSERT(!fields.empty(), "Cannot use this constructor with empty fields");
  return StructType::Get(fields[0]->context(), std::move(fields));
}

FunctionType *FunctionType::Get(util::RegionVector<Type *> &&params,
                                Type *ret) {
  // TODO(pmenon): Use cache
  return new (ret->context().region()) FunctionType(std::move(params), ret);
}

}  // namespace tpl::ast