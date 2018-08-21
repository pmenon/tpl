
#include <ast/ast_context.h>

#include "ast/ast_context.h"

#include "ast/type.h"
#include "util/string_map.h"

namespace tpl::ast {

struct AstContext::Implementation : public util::RegionObject {
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

  util::StringMap<char, util::RegionAllocator<char>> string_table_;

  util::RegionUnorderedMap<ast::Identifier, ast::Type *, ast::IdentifierHasher,
                           ast::IdentifierEquality>
      builtin_types_;

  template <typename T, typename U>
  struct PairHash {
    uint32_t operator()(const std::pair<T, U> &p) const {
      uint64_t key = static_cast<uint64_t>(std::hash<T>()(p.first)) << 32 |
                     static_cast<uint64_t>(std::hash<U>()(p.second));
      key += ~(key << 32);
      key ^= (key >> 22);
      key += ~(key << 13);
      key ^= (key >> 8);
      key += (key << 3);
      key ^= (key >> 15);
      key += ~(key << 27);
      key ^= (key >> 31);
      return static_cast<uint32_t>(key);
    }
  };

  // TODO: Switch to something better for small key/value types?
  util::RegionUnorderedMap<Type *, PointerType *> pointer_types_;
  util::RegionUnorderedMap<std::pair<Type *, uint64_t>, ArrayType *,
                           PairHash<Type *, uint64_t>>
      array_types_;

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
                      util::RegionAllocator<char>(ctx.region())),
        builtin_types_(ctx.region()),
        pointer_types_(ctx.region()),
        array_types_(ctx.region()) {}
};

AstContext::AstContext(util::Region &region, ast::AstNodeFactory &node_factory,
                       sema::ErrorReporter &error_reporter)
    : region_(region),
      node_factory_(node_factory),
      error_reporter_(error_reporter) {
  impl_ = new (region) Implementation(*this);

  // Initialize builtin types
  impl().builtin_types_.insert({
      // Boolean
      {GetIdentifier("bool"), &impl().boolean},
      // Nil
      {GetIdentifier("nil"), &impl().nil},
      // Integers
      {GetIdentifier("int8"), &impl().int8},
      {GetIdentifier("int16"), &impl().int16},
      {GetIdentifier("int32"), &impl().int32},
      {GetIdentifier("int64"), &impl().int64},
      {GetIdentifier("uint8"), &impl().uint8},
      {GetIdentifier("uint16"), &impl().uint16},
      {GetIdentifier("uint32"), &impl().uint32},
      {GetIdentifier("uint64"), &impl().uint64},
      {GetIdentifier("float32"), &impl().float32},
      {GetIdentifier("float64"), &impl().float64},
      // Typedefs
      {GetIdentifier("int"), &impl().int32},
      {GetIdentifier("float"), &impl().float32},
      {GetIdentifier("void"), &impl().nil},
  });
}

Identifier AstContext::GetIdentifier(util::StringRef str) {
  if (str.empty()) return Identifier(nullptr);

  auto iter = impl().string_table_.insert({str, char(0)});
  return Identifier(iter.first->key(), iter.first->key_length());
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

  cached_types.emplace(base, pointer_type);

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

  cached_types.emplace(std::make_pair(elem_type, length), array_type);

  return array_type;
}

StructType *StructType::Get(AstContext &ctx,
                            util::RegionVector<Type *> &&fields) {
  // TODO(pmenon): Use cache?
  return new (ctx.region()) StructType(ctx, std::move(fields));
}

StructType *StructType::Get(util::RegionVector<Type *> &&fields) {
  TPL_ASSERT(!fields.empty() &&
             "Cannot use this constructor with empty fields");
  return StructType::Get(fields[0]->context(), std::move(fields));
}

FunctionType *FunctionType::Get(util::RegionVector<Type *> &&params,
                                Type *ret) {
  // TODO(pmenon): Use cache
  return new (ret->context().region()) FunctionType(std::move(params), ret);
}

}  // namespace tpl::ast