#pragma once

#include "ast/context.h"
#include "ast/type.h"

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
#include "sql/vector_filter_executor.h"
#include "util/csv_reader.h"

namespace tpl::ast {

/**
 * Type builders provide a simple API for building TPL types for C/C++ types known at compile time.
 * @tparam T The C/C++ type to build.
 */
template <typename T>
class TypeBuilder {};

/**
 * Const C/C++ types don't have counterparts in TPL.
 */
template <typename T>
class TypeBuilder<const T> : public TypeBuilder<T> {};

/**
 * Volatile C/C++ types don't have counterparts in TPL.
 **/
template <typename T>
class TypeBuilder<volatile T> : public TypeBuilder<T> {};

/**
 * Const-volatile C/C++ types don't have counterparts in TPL.
 **/
template <typename T>
class TypeBuilder<const volatile T> : public TypeBuilder<T> {};

/**
 * Pointers.
 */
template <typename T>
class TypeBuilder<T *> {
 public:
  static PointerType *Get(Context *context) {
    return PointerType::Get(TypeBuilder<T>::Get(context));
  }
};

/**
 * References don't exist in TPL.
 */
template <typename T>
class TypeBuilder<T &> {};

/**
 * Arrays with a known compile-time size.
 * @tparam T The type of the elements in the array.
 */
template <typename T, std::size_t N>
class TypeBuilder<T[N]> {
 public:
  static ArrayType *Get(Context *context) {
    return ArrayType::Get(N, TypeBuilder<T>::Get(context));
  }
};

/**
 * Arrays with an unknown compile-time size.
 * @tparam T The type of the elements in the array.
 */
template <typename T>
class TypeBuilder<T[]> {
 public:
  static ArrayType *Get(Context *context) {
    return ArrayType::Get(0, TypeBuilder<T>::Get(context));
  }
};

/**
 * Specialize for std::array<> and alias to the raw array type.
 * @tparam T The type of the elements in the array.
 * @tparam N The number of elements in the array.
 */
template <typename T, std::size_t N>
class TypeBuilder<std::array<T, N>> : public TypeBuilder<T[N]> {};

/**
 * Specialize for std::array<> and alias to the raw array type.
 * @tparam T
 */
template <typename T>
class TypeBuilder<std::array<T, 0>> : public TypeBuilder<T[]> {};

#define HANDLE_BUILTIN_TYPE(kind, ctype, ...)                                             \
  /**                                                                                     \
   * Specialized builder for primitive types.                                             \
   */                                                                                     \
  template <>                                                                             \
  class TypeBuilder<                                                                      \
      std::conditional_t<ast::BuiltinType::kind == ast::BuiltinType::Nil, void, ctype>> { \
   public:                                                                                \
    static BuiltinType *Get(Context *context) {                                           \
      return BuiltinType::Get(context, ast::BuiltinType::kind);                           \
    }                                                                                     \
  };
BUILTIN_TYPE_LIST(HANDLE_BUILTIN_TYPE, HANDLE_BUILTIN_TYPE, HANDLE_BUILTIN_TYPE)
#undef HANDLE_BUILTIN_TYPE

/**
 * Specialize for std::byte, which is a TPL type. Alias to uint8_t.
 */
template <>
class TypeBuilder<byte> : public TypeBuilder<uint8_t> {};

/**
 * void* isn't a TPL type, but occurs enough that we special-case it here.
 */
template <>
class TypeBuilder<void *> : public TypeBuilder<byte *> {};

/**
 * const void* isn't a TPL type, but occurs enough that we special-case it here.
 */
template <>
class TypeBuilder<const void *> : public TypeBuilder<byte *> {};

/**
 * volatile void* isn't a TPL type, but occurs enough that we special-case it here.
 */
template <>
class TypeBuilder<volatile void *> : public TypeBuilder<byte *> {};

/**
 * const volatile void* isn't a TPL type, but occurs enough that we special-case it here.
 */
template <>
class TypeBuilder<const volatile void *> : public TypeBuilder<byte *> {};

/**
 * Use std::string as TPL strings.
 */
template <>
class TypeBuilder<std::string> {
 public:
  static StringType *Get(Context *context) { return StringType::Get(context); }
};

}  // namespace tpl::ast
