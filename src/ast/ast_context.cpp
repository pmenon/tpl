#include "ast/ast_context.h"

#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/StringMap.h"

#include "ast/ast_node_factory.h"
#include "ast/type.h"
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
        string_table_(kDefaultStringTableCapacity,
                      util::LlvmRegionAllocator(ctx.region())) {}
};

AstContext::AstContext(util::Region &region,
                       sema::ErrorReporter &error_reporter)
    : region_(region),
      error_reporter_(error_reporter),
      node_factory_(std::make_unique<AstNodeFactory>(region)),
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

  // Compute size and alignment. Alignment of struct is alignment of largest
  // struct element.
  size_t size = 0;
  size_t alignment = 0;
  for (const auto *type : fields) {
    // Check if the type needs to be padded
    if (size & (type->alignment() - 1)) {
      size = util::MathUtil::AlignTo(size, type->alignment());
    }

    // Update size and calculate alignment
    size += type->size();
    alignment = std::max(alignment, type->alignment());
  }

  // Done
  return new (ctx.region()) StructType(ctx, size, alignment, std::move(fields));
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