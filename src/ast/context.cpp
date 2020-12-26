#include "ast/context.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

// LLVM data structures.
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallString.h"
#include "llvm/ADT/StringMap.h"

// For string formatting.
#include "spdlog/fmt/fmt.h"

#include "ast/ast_node_factory.h"
#include "ast/builtins.h"
#include "ast/type.h"
#include "common/common.h"
#include "sql/aggregation_hash_table.h"
#include "sql/aggregators.h"
#include "sql/compact_storage.h"
#include "sql/execution_context.h"
#include "sql/filter_manager.h"
#include "sql/generic_value.h"
#include "sql/join_hash_table.h"
#include "sql/runtime_types.h"
#include "sql/sorter.h"
#include "sql/table_vector_iterator.h"
#include "sql/thread_state_container.h"
#include "sql/value.h"
#include "util/csv_reader.h"
#include "util/math_util.h"
#include "util/region_allocator_llvm.h"

namespace tpl::ast {

// ---------------------------------------------------------
// Key type used in the cache for struct types in the context
// ---------------------------------------------------------

// Compute a hash_code for a field
llvm::hash_code hash_value(const Field &field) {
  return llvm::hash_combine(field.name.GetData(), field.type);
}

/*
 * Struct required to store TPL struct types in LLVM DenseMaps.
 */
struct StructTypeKeyInfo {
  struct KeyTy {
    const util::RegionVector<Field> &elements;

    explicit KeyTy(const util::RegionVector<Field> &es) : elements(es) {}

    explicit KeyTy(const StructType *struct_type) : elements(struct_type->GetFields()) {}

    bool operator==(const KeyTy &that) const { return elements == that.elements; }

    bool operator!=(const KeyTy &that) const { return !this->operator==(that); }
  };

  static StructType *getEmptyKey() { return llvm::DenseMapInfo<StructType *>::getEmptyKey(); }

  static StructType *getTombstoneKey() {
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

  static bool isEqual(const StructType *lhs, const StructType *rhs) { return lhs == rhs; }
};

// ---------------------------------------------------------
// Key type used in the cache for function types in the context
// ---------------------------------------------------------

/*
 * Struct required to store TPL function types in LLVM DenseMaps.
 */
struct FunctionTypeKeyInfo {
  struct KeyTy {
    Type *const ret_type;
    const util::RegionVector<Field> &params;

    explicit KeyTy(Type *ret_type, const util::RegionVector<Field> &ps)
        : ret_type(ret_type), params(ps) {}

    explicit KeyTy(const FunctionType *func_type)
        : ret_type(func_type->GetReturnType()), params(func_type->GetParams()) {}

    bool operator==(const KeyTy &that) const {
      return ret_type == that.ret_type && params == that.params;
    }

    bool operator!=(const KeyTy &that) const { return !this->operator==(that); }
  };

  static FunctionType *getEmptyKey() { return llvm::DenseMapInfo<FunctionType *>::getEmptyKey(); }

  static FunctionType *getTombstoneKey() {
    return llvm::DenseMapInfo<FunctionType *>::getTombstoneKey();
  }

  static std::size_t getHashValue(const KeyTy &key) {
    return llvm::hash_combine(key.ret_type,
                              llvm::hash_combine_range(key.params.begin(), key.params.end()));
  }

  static std::size_t getHashValue(const FunctionType *func_type) {
    return getHashValue(KeyTy(func_type));
  }

  static bool isEqual(const KeyTy &lhs, const FunctionType *rhs) {
    if (rhs == getEmptyKey() || rhs == getTombstoneKey()) return false;
    return lhs == KeyTy(rhs);
  }

  static bool isEqual(const FunctionType *lhs, const FunctionType *rhs) { return lhs == rhs; }
};

struct Context::Implementation {
  static constexpr const uint32_t kDefaultTableCapacity = 32;

  // -------------------------------------------------------
  // Builtin types
  // -------------------------------------------------------

#define F(BKind, ...) BuiltinType *BKind##Type;
  BUILTIN_TYPE_LIST(F, F, F)
#undef F
  StringType *string_type;

  // -------------------------------------------------------
  // Type caches
  // -------------------------------------------------------

  llvm::StringMap<llvm::NoneType, util::LLVMRegionAllocator> identifier_table;
  std::vector<BuiltinType *> builtin_types_list;
  llvm::DenseMap<Identifier, Type *> builtin_types;
  llvm::DenseMap<Identifier, Builtin> builtin_funcs;
  llvm::DenseMap<Type *, PointerType *> pointer_types;
  llvm::DenseMap<std::pair<Type *, uint64_t>, ArrayType *> array_types;
  llvm::DenseMap<std::pair<Type *, Type *>, MapType *> map_types;
  llvm::DenseSet<StructType *, StructTypeKeyInfo> anonymous_struct_types;
  llvm::StringMap<StructType *, util::LLVMRegionAllocator> named_struct_types;
  uint64_t named_struct_types_unique_id;
  llvm::DenseSet<FunctionType *, FunctionTypeKeyInfo> func_types;

  explicit Implementation(Context *ctx)
      : identifier_table(kDefaultTableCapacity, util::LLVMRegionAllocator(ctx->GetRegion())),
        named_struct_types(kDefaultTableCapacity, util::LLVMRegionAllocator(ctx->GetRegion())),
        named_struct_types_unique_id(0) {
    // Instantiate all the builtin types.
#define F(BKind, CppType, ...)         \
  BKind##Type = new (ctx->GetRegion()) \
      BuiltinType(ctx, sizeof(CppType), alignof(CppType), BuiltinType::BKind);
    BUILTIN_TYPE_LIST(F, F, F)
#undef F

    string_type = new (ctx->GetRegion()) StringType(ctx);
  }
};

Context::Context(sema::ErrorReporter *error_reporter)
    : region_("ast-mem-region"),
      error_reporter_(error_reporter),
      node_factory_(std::make_unique<AstNodeFactory>(&region_)),
      impl_(std::make_unique<Implementation>(this)) {
  // Put all builtin types into list.
  impl_->builtin_types_list.resize(BuiltinType::GetNumBuiltinKinds());
#define F(BKind, ...) impl_->builtin_types_list[BuiltinType::BKind] = Impl()->BKind##Type;
  BUILTIN_TYPE_LIST(F, F, F)
#undef F

  // Put all builtin types into fast map keyed on its name.
  impl_->builtin_types.reserve(BuiltinType::GetNumBuiltinKinds());
#define PRIM(BKind, CppType, TplName) \
  impl_->builtin_types[GetIdentifier(TplName)] = Impl()->BKind##Type;
#define OTHERS(BKind, CppType) impl_->builtin_types[GetIdentifier(#BKind)] = Impl()->BKind##Type;
  BUILTIN_TYPE_LIST(PRIM, OTHERS, OTHERS)
#undef OTHERS
#undef PRIM

  // Insert some convenient type-aliases.
  impl_->builtin_types[GetIdentifier("int")] = Impl()->Int32Type;
  impl_->builtin_types[GetIdentifier("float")] = Impl()->Float32Type;
  impl_->builtin_types[GetIdentifier("void")] = Impl()->NilType;

  // Put all builtin functions into a fast map keyed on name.
  impl_->builtin_funcs.reserve(Builtins::NumBuiltins());
#define F(Name, ...) \
  impl_->builtin_funcs[GetIdentifier(Builtins::GetFunctionName(Builtin::Name))] = Builtin::Name;
  BUILTINS_LIST(F)
#undef F
}

Context::~Context() = default;

Identifier Context::GetIdentifier(std::string_view str) {
  if (str.empty()) {
    return Identifier();
  }

  auto &entry = *impl_->identifier_table.insert(std::make_pair(str, llvm::None)).first;
  return Identifier(&entry);
}

std::size_t Context::GetNumIdentifiers() const noexcept { return impl_->identifier_table.size(); }

Type *Context::LookupBuiltinType(Identifier name) const {
  const auto iter = impl_->builtin_types.find(name);
  return (iter == impl_->builtin_types.end() ? nullptr : iter->second);
}

bool Context::IsBuiltinFunction(Identifier name, Builtin *builtin) const {
  if (const auto iter = impl_->builtin_funcs.find(name); iter != impl_->builtin_funcs.end()) {
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
  return ctx->Impl()->builtin_types_list[kind];
}

// static
StringType *StringType::Get(Context *ctx) { return ctx->Impl()->string_type; }

// static
PointerType *PointerType::Get(Type *base) {
  Context *ctx = base->GetContext();

  PointerType *&pointer_type = ctx->Impl()->pointer_types[base];

  if (pointer_type == nullptr) {
    pointer_type = new (ctx->GetRegion()) PointerType(base);
  }

  return pointer_type;
}

// static
ArrayType *ArrayType::Get(uint64_t length, Type *elem_type) {
  Context *ctx = elem_type->GetContext();

  ArrayType *&array_type = ctx->Impl()->array_types[{elem_type, length}];

  if (array_type == nullptr) {
    array_type = new (ctx->GetRegion()) ArrayType(length, elem_type);
  }

  return array_type;
}

// static
MapType *MapType::Get(Type *key_type, Type *value_type) {
  Context *ctx = key_type->GetContext();

  MapType *&map_type = ctx->Impl()->map_types[{key_type, value_type}];

  if (map_type == nullptr) {
    map_type = new (ctx->GetRegion()) MapType(key_type, value_type);
  }

  return map_type;
}

// static
StructType *StructType::Get(Context *ctx, util::RegionVector<Field> &&fields) {
  // Empty structs get an artificial element.
  if (fields.empty()) {
    // Empty structs get an artificial byte field to ensure non-zero size.
    ast::Identifier name = ctx->GetIdentifier("__field$0$");
    ast::Type *byte_type = ast::BuiltinType::Get(ctx, ast::BuiltinType::Int8);
    fields.emplace_back(name, byte_type);
  }

  const StructTypeKeyInfo::KeyTy key(fields);

  // To avoid two lookups into the map, here we instead lookup based on 'key'
  // and update the reference to the struct type in-place to a fresh struct
  // ONLY IF one isn't found.
  auto [iter, inserted] = ctx->Impl()->anonymous_struct_types.insert_as(nullptr, key);

  StructType *struct_type;

  if (inserted) {
    // The struct wasn't found. Allocate ona dn update the map in-place.
    LayoutHelper helper(fields);
    const auto &offsets = helper.GetOffsets();
    struct_type = new (ctx->GetRegion()) StructType(
        ctx, helper.GetSize(), helper.GetAlignment(), ast::Identifier{}, std::move(fields),
        util::RegionVector<uint32_t>{offsets.begin(), offsets.end(), ctx->GetRegion()});
    *iter = struct_type;
  } else {
    // The struct type was found, so return it.
    struct_type = *iter;
  }

  return struct_type;
}

// static
StructType *StructType::Get(util::RegionVector<Field> &&fields) {
  TPL_ASSERT(!fields.empty(), "Cannot use StructType::Get(fields) with an empty list of fields");
  return StructType::Get(fields[0].type->GetContext(), std::move(fields));
}

// static
StructType *StructType::Get(Context *ctx, ast::Identifier name,
                            util::RegionVector<Field> &&fields) {
  // Lookup the name in the named struct types map to ensure uniqueness.
  auto iter_bool = ctx->Impl()->named_struct_types.insert(std::make_pair(name.GetView(), nullptr));

  // While there is a name collision, try to find a unique version.
  if (!iter_bool.second) {
    llvm::SmallString<64> temp(name.GetView());
    temp.push_back('_');
    const auto name_len = name.GetLength();
    do {
      fmt::format_int int_str(ctx->Impl()->named_struct_types_unique_id++);
      temp.resize(name_len + 1);
      temp.append(std::string_view(int_str.data(), int_str.size()));

      iter_bool = ctx->Impl()->named_struct_types.insert(std::make_pair(temp.str(), nullptr));
    } while (!iter_bool.second);

    // The name.
    name = ctx->GetIdentifier(temp.str());
  }

  // Compute the struct layout.
  LayoutHelper helper(fields);
  const auto &offsets = helper.GetOffsets();

  StructType *struct_type = new (ctx->GetRegion())
      StructType(ctx, helper.GetSize(), helper.GetAlignment(), name, std::move(fields),
                 util::RegionVector<uint32_t>{offsets.begin(), offsets.end(), ctx->GetRegion()});

  iter_bool.first->setValue(struct_type);

  return struct_type;
}

// static
StructType *StructType::Get(ast::Identifier name, util::RegionVector<Field> &&fields) {
  TPL_ASSERT(!fields.empty(),
             "Cannot use StructType::Get(name, fields) with an empty list of fields");
  return StructType::Get(fields[0].type->GetContext(), name, std::move(fields));
}

// static
FunctionType *FunctionType::Get(Type *ret) {
  util::RegionVector<Field> empty(ret->GetContext()->GetRegion());
  return FunctionType::Get(std::move(empty), ret);
}

// static
FunctionType *FunctionType::Get(util::RegionVector<Field> &&params, Type *ret) {
  Context *ctx = ret->GetContext();

  const FunctionTypeKeyInfo::KeyTy key(ret, params);

  auto [iter, inserted] = ctx->Impl()->func_types.insert_as(nullptr, key);

  FunctionType *func_type = nullptr;

  if (inserted) {
    // The function type was not in the cache, create the type now and insert it
    // into the cache
    func_type = new (ctx->GetRegion()) FunctionType(std::move(params), ret);
    *iter = func_type;
  } else {
    func_type = *iter;
  }

  return func_type;
}

}  // namespace tpl::ast
