#include "ast/context.h"

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/StringMap.h"

#include "ast/ast_node_factory.h"
#include "ast/builtins.h"
#include "ast/type.h"
#include "sql/aggregation_hash_table.h"
#include "sql/aggregators.h"
#include "sql/execution_context.h"
#include "sql/filter_manager.h"
#include "sql/generic_value.h"
#include "sql/join_hash_table.h"
#include "sql/join_hash_table_vector_probe.h"
#include "sql/sorter.h"
#include "sql/table_vector_iterator.h"
#include "sql/thread_state_container.h"
#include "sql/value.h"
#include "sql/vector_filter_executor.h"
#include "util/common.h"
#include "util/math_util.h"

namespace tpl::ast {

// ---------------------------------------------------------
// Key type used in the cache for struct types in the context
// ---------------------------------------------------------

/// Compute a hash_code for a field
llvm::hash_code hash_value(const Field &field) {
  return llvm::hash_combine(field.name.data(), field.type);
}

struct StructTypeKeyInfo {
  struct KeyTy {
    const util::RegionVector<Field> &elements;

    explicit KeyTy(const util::RegionVector<Field> &es) : elements(es) {}

    explicit KeyTy(const StructType *struct_type)
        : elements(struct_type->fields()) {}

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
    Type *const ret_type;
    const util::RegionVector<Field> &params;

    explicit KeyTy(Type *ret_type, const util::RegionVector<Field> &ps)
        : ret_type(ret_type), params(ps) {}

    explicit KeyTy(const FunctionType *func_type)
        : ret_type(func_type->return_type()), params(func_type->params()) {}

    bool operator==(const KeyTy &that) const {
      return ret_type == that.ret_type && params == that.params;
    }

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

struct Context::Implementation {
  static constexpr const u32 kDefaultStringTableCapacity = 32;

  // -------------------------------------------------------
  // Builtin types
  // -------------------------------------------------------

#define F(BKind, ...) BuiltinType *BKind##Type;
  BUILTIN_TYPE_LIST(F, F, F)
#undef F
  StringType *string;

  // -------------------------------------------------------
  // Type caches
  // -------------------------------------------------------

  llvm::StringMap<char, util::LLVMRegionAllocator> string_table;
  std::vector<BuiltinType *> builtin_types_list;
  llvm::DenseMap<Identifier, Type *> builtin_types;
  llvm::DenseMap<Identifier, Builtin> builtin_funcs;
  llvm::DenseMap<Type *, PointerType *> pointer_types;
  llvm::DenseMap<std::pair<Type *, u64>, ArrayType *> array_types;
  llvm::DenseMap<std::pair<Type *, Type *>, MapType *> map_types;
  llvm::DenseSet<StructType *, StructTypeKeyInfo> struct_types;
  llvm::DenseSet<FunctionType *, FunctionTypeKeyInfo> func_types;

  explicit Implementation(Context *ctx)
      : string_table(kDefaultStringTableCapacity,
                     util::LLVMRegionAllocator(ctx->region())) {
    // Instantiate all the builtins
#define F(BKind, CppType, ...)      \
  BKind##Type = new (ctx->region()) \
      BuiltinType(ctx, sizeof(CppType), alignof(CppType), BuiltinType::BKind);
    BUILTIN_TYPE_LIST(F, F, F)
#undef F

    string = new (ctx->region()) StringType(ctx);
  }
};

Context::Context(util::Region *region, sema::ErrorReporter *error_reporter)
    : region_(region),
      error_reporter_(error_reporter),
      node_factory_(std::make_unique<AstNodeFactory>(region)),
      impl_(std::make_unique<Implementation>(this)) {
  // Put all builtins into list
#define F(BKind, ...) impl()->builtin_types_list.push_back(impl()->BKind##Type);
  BUILTIN_TYPE_LIST(F, F, F)
#undef F

  // Put all builtins into cache by name
#define PRIM(BKind, CppType, TplName) \
  impl()->builtin_types[GetIdentifier(TplName)] = impl()->BKind##Type;
#define OTHERS(BKind, CppType) \
  impl()->builtin_types[GetIdentifier(#BKind)] = impl()->BKind##Type;
  BUILTIN_TYPE_LIST(PRIM, OTHERS, OTHERS)
#undef OTHERS
#undef PRIM

  // Builtin aliases
  impl()->builtin_types[GetIdentifier("int")] = impl()->Int32Type;
  impl()->builtin_types[GetIdentifier("float")] = impl()->Float32Type;
  impl()->builtin_types[GetIdentifier("void")] = impl()->NilType;

  // Initialize builtin functions
#define BUILTIN_FUNC(Name, ...)        \
  impl()->builtin_funcs[GetIdentifier( \
      Builtins::GetFunctionName(Builtin::Name))] = Builtin::Name;
  BUILTINS_LIST(BUILTIN_FUNC)
#undef BUILTIN_FUNC
}

Context::~Context() = default;

Identifier Context::GetIdentifier(llvm::StringRef str) {
  if (str.empty()) {
    return Identifier(nullptr);
  }

  auto iter =
      impl()
          ->string_table.insert(std::make_pair(str, static_cast<char>(0)))
          .first;
  return Identifier(iter->getKeyData());
}

Type *Context::LookupBuiltinType(Identifier identifier) const {
  auto iter = impl()->builtin_types.find(identifier);
  return (iter == impl()->builtin_types.end() ? nullptr : iter->second);
}

bool Context::IsBuiltinFunction(Identifier identifier, Builtin *builtin) const {
  if (auto iter = impl()->builtin_funcs.find(identifier);
      iter != impl()->builtin_funcs.end()) {
    if (builtin != nullptr) {
      *builtin = iter->second;
    }
    return true;
  }

  return false;
}

PointerType *Type::PointerTo() { return PointerType::Get(this); }

// static
BuiltinType *BuiltinType::Get(Context *ctx, BuiltinType::Kind kind) {
  return ctx->impl()->builtin_types_list[kind];
}

// static
StringType *StringType::Get(Context *ctx) { return ctx->impl()->string; }

// static
PointerType *PointerType::Get(Type *base) {
  Context *ctx = base->context();

  PointerType *&pointer_type = ctx->impl()->pointer_types[base];

  if (pointer_type == nullptr) {
    pointer_type = new (ctx->region()) PointerType(base);
  }

  return pointer_type;
}

// static
ArrayType *ArrayType::Get(u64 length, Type *elem_type) {
  Context *ctx = elem_type->context();

  ArrayType *&array_type = ctx->impl()->array_types[{elem_type, length}];

  if (array_type == nullptr) {
    array_type = new (ctx->region()) ArrayType(length, elem_type);
  }

  return array_type;
}

// static
MapType *MapType::Get(Type *key_type, Type *value_type) {
  Context *ctx = key_type->context();

  MapType *&map_type = ctx->impl()->map_types[{key_type, value_type}];

  if (map_type == nullptr) {
    map_type = new (ctx->region()) MapType(key_type, value_type);
  }

  return map_type;
}

// static
StructType *StructType::Get(Context *ctx, util::RegionVector<Field> &&fields) {
  // Empty structs get an artificial element
  if (fields.empty()) {
    // Empty structs get an artificial byte field to ensure non-zero size
    ast::Identifier name = ctx->GetIdentifier("__field$0$");
    ast::Type *byte_type = ast::BuiltinType::Get(ctx, ast::BuiltinType::Int8);
    fields.emplace_back(name, byte_type);
  }

  const StructTypeKeyInfo::KeyTy key(fields);

  auto [iter, inserted] = ctx->impl()->struct_types.insert_as(nullptr, key);

  StructType *struct_type = nullptr;

  if (inserted) {
    // Compute size and alignment. Alignment of struct is alignment of largest
    // struct element.
    u32 size = 0;
    u32 alignment = 0;
    util::RegionVector<u32> field_offsets(ctx->region());
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

    // Empty structs have an alignment of 1 byte
    if (alignment == 0) {
      alignment = 1;
    }

    // Add padding at end so that these structs can be placed compactly in an
    // array and still respect alignment
    if (!util::MathUtil::IsAligned(size, alignment)) {
      size = static_cast<u32>(util::MathUtil::AlignTo(size, alignment));
    }

    // Create type
    struct_type = new (ctx->region()) StructType(
        ctx, size, alignment, std::move(fields), std::move(field_offsets));

    // Set in cache
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
  Context *ctx = ret->context();

  const FunctionTypeKeyInfo::KeyTy key(ret, params);

  auto [iter, inserted] = ctx->impl()->func_types.insert_as(nullptr, key);

  FunctionType *func_type = nullptr;

  if (inserted) {
    // The function type was not in the cache, create the type now and insert it
    // into the cache
    func_type = new (ctx->region()) FunctionType(std::move(params), ret);
    *iter = func_type;
  } else {
    func_type = *iter;
  }

  return func_type;
}

}  // namespace tpl::ast
