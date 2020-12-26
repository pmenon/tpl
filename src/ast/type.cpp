#include "ast/type.h"

#include <unordered_map>
#include <utility>

#include "sql/aggregation_hash_table.h"
#include "sql/aggregators.h"
#include "sql/compact_storage.h"
#include "sql/execution_context.h"
#include "sql/filter_manager.h"
#include "sql/hash_table_entry.h"
#include "sql/join_hash_table.h"
#include "sql/sorter.h"
#include "sql/table_vector_iterator.h"
#include "sql/thread_state_container.h"
#include "sql/value.h"
#include "sql/vector_filter_executor.h"
#include "sql/vector_projection_iterator.h"
#include "util/csv_reader.h"
#include "util/math_util.h"

namespace tpl::ast {

// ---------------------------------------------------------
// Type
// ---------------------------------------------------------

// TODO(pmenon): Fix me
bool Type::IsArithmetic() const {
  return IsIntegerType() ||                             // Primitive TPL integers
         IsFloatType() ||                               // Primitive TPL floats
         IsSpecificBuiltin(BuiltinType::IntegerVal) ||  // SQL integer
         IsSpecificBuiltin(BuiltinType::RealVal) ||     // SQL reals
         IsSpecificBuiltin(BuiltinType::DecimalVal);    // SQL decimals
}

// ---------------------------------------------------------
// Builtin Type
// ---------------------------------------------------------

const char *BuiltinType::kTplNames[] = {
#define PRIM(BKind, CppType, Name, ...) Name,
#define OTHERS(BKind, ...) #BKind,
    BUILTIN_TYPE_LIST(PRIM, OTHERS, OTHERS)
#undef F
};

const char *BuiltinType::kCppNames[] = {
#define F(BKind, CppType, ...) #CppType,
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

const uint64_t BuiltinType::kSizes[] = {
#define F(BKind, CppType, ...) sizeof(CppType),
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

const uint64_t BuiltinType::kAlignments[] = {
#define F(Kind, CppType, ...) std::alignment_of_v<CppType>,
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

const bool BuiltinType::kPrimitiveFlags[] = {
#define F(Kind, CppType, ...) std::is_fundamental_v<CppType>,
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

const bool BuiltinType::kFloatingPointFlags[] = {
#define F(Kind, CppType, ...) std::is_floating_point_v<CppType>,
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

const bool BuiltinType::kSignedFlags[] = {
#define F(Kind, CppType, ...) std::is_signed_v<CppType>,
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

// ---------------------------------------------------------
// Function Type
// ---------------------------------------------------------

FunctionType::FunctionType(util::RegionVector<Field> &&params, Type *ret)
    : Type(ret->GetContext(), sizeof(void *), alignof(void *), TypeId::FunctionType),
      params_(std::move(params)),
      ret_(ret) {}

// ---------------------------------------------------------
// Map Type
// ---------------------------------------------------------

MapType::MapType(Type *key_type, Type *val_type)
    : Type(key_type->GetContext(), sizeof(std::unordered_map<int32_t, int32_t>),
           alignof(std::unordered_map<int32_t, int32_t>), TypeId::MapType),
      key_type_(key_type),
      val_type_(val_type) {}

// ---------------------------------------------------------
// Struct Type
// ---------------------------------------------------------

StructType::LayoutHelper::LayoutHelper(const util::RegionVector<Field> &fields)
    : size_(0), alignment_(0) {
  offsets_.reserve(fields.size());
  for (const auto &field : fields) {
    // Check if the type needs to be padded.
    uint32_t field_align = field.type->GetAlignment();
    if (!util::MathUtil::IsAligned(size_, field_align)) {
      size_ = static_cast<uint32_t>(util::MathUtil::AlignTo(size_, field_align));
    }

    // Update size and calculate alignment.
    offsets_.push_back(size_);
    size_ += field.type->GetSize();
    alignment_ = std::max(alignment_, field.type->GetAlignment());
  }

  // Empty structs have an alignment of 1 byte.
  if (alignment_ == 0) {
    alignment_ = 1;
  }

  // Add padding at end so that these structs can be placed compactly in an
  // array and still respect alignment.
  if (!util::MathUtil::IsAligned(size_, alignment_)) {
    size_ = static_cast<uint32_t>(util::MathUtil::AlignTo(size_, alignment_));
  }
}

StructType::StructType(Context *ctx, uint32_t size, uint32_t alignment,ast::Identifier name,
                       util::RegionVector<Field> &&fields,
                       util::RegionVector<uint32_t> &&field_offsets)
    : Type(ctx, size, alignment, TypeId::StructType),
      name_(name),
      fields_(std::move(fields)),
      field_offsets_(std::move(field_offsets)) {}

}  // namespace tpl::ast
