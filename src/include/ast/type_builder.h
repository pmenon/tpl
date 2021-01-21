#pragma once

#include "ast/context.h"
#include "ast/type.h"
#include "ast/type_proxy.h"

namespace tpl::ast {

/**
 * Type builders provide a simple API for building TPL types for C/C++ types known at compile time.
 * @tparam T The C/C++ type to build.
 */
template <typename T, typename Enable = void>
struct TypeBuilder {};

/**
 * Const C/C++ types don't have counterparts in TPL.
 */
template <typename T>
struct TypeBuilder<const T> : TypeBuilder<T> {};

/**
 * Volatile C/C++ types don't have counterparts in TPL.
 **/
template <typename T>
struct TypeBuilder<volatile T> : TypeBuilder<T> {};

/**
 * Const-volatile C/C++ types don't have counterparts in TPL.
 **/
template <typename T>
struct TypeBuilder<const volatile T> : TypeBuilder<T> {};

/**
 * Pointers.
 */
template <typename T>
struct TypeBuilder<T *> {
  static PointerType *Get(Context *context) {
    return PointerType::Get(TypeBuilder<T>::Get(context));
  }
};

/**
 * References don't exist in TPL.
 */
template <typename T>
struct TypeBuilder<T &> {};

/**
 * Arrays with a known compile-time size.
 * @tparam T The type of the elements in the array.
 */
template <typename T, std::size_t N>
struct TypeBuilder<T[N]> {
  static ArrayType *Get(Context *context) {
    return ArrayType::Get(N, TypeBuilder<T>::Get(context));
  }
};

/**
 * Arrays with an unknown compile-time size.
 * @tparam T The type of the elements in the array.
 */
template <typename T>
struct TypeBuilder<T[]> {
  static ArrayType *Get(Context *context) {
    return ArrayType::Get(0, TypeBuilder<T>::Get(context));
  }
};

/**
 * Specialize for primitive builtin types. These are C++ primitive types exposed as TPL primitives.
 */
#define F(kind, ctype, ...)                                                               \
  template <>                                                                             \
  struct TypeBuilder<                                                                     \
      std::conditional_t<ast::BuiltinType::kind == ast::BuiltinType::Nil, void, ctype>> { \
    static BuiltinType *Get(Context *context) {                                           \
      return BuiltinType::Get(context, ast::BuiltinType::kind);                           \
    }                                                                                     \
  };
PRIMITIVE_BUILTIN_TYPE_LIST(F)
#undef F

/**
 * Specialize for the other TPL builtin types that have non-primitive C++ implementations.
 */
#define F(kind, ...)                                            \
  template <>                                                   \
  struct TypeBuilder<tpl::ast::x::kind> {                       \
    static BuiltinType *Get(Context *context) {                 \
      return BuiltinType::Get(context, ast::BuiltinType::kind); \
    }                                                           \
  };
NON_PRIMITIVE_BUILTIN_TYPE_LIST(F)
SQL_BUILTIN_TYPE_LIST(F)
#undef F

/**
 * Specialize for std::byte, which is a TPL type. Alias to uint8_t.
 */
template <>
struct TypeBuilder<byte> : TypeBuilder<uint8_t> {};

/**
 * void* isn't a TPL type, but occurs enough that we special-case it here.
 */
template <>
struct TypeBuilder<void *> : TypeBuilder<byte *> {};

/**
 * const void* isn't a TPL type, but occurs enough that we special-case it here.
 */
template <>
struct TypeBuilder<const void *> : TypeBuilder<byte *> {};

/**
 * volatile void* isn't a TPL type, but occurs enough that we special-case it here.
 */
template <>
struct TypeBuilder<volatile void *> : TypeBuilder<byte *> {};

/**
 * const volatile void* isn't a TPL type, but occurs enough that we special-case it here.
 */
template <>
struct TypeBuilder<const volatile void *> : TypeBuilder<byte *> {};

/**
 * Specialize a nullptr as Nil.
 */
template <>
struct TypeBuilder<std::nullptr_t> : TypeBuilder<void> {};

/**
 * Specialize std::string_view (or anything convertible to it) as a TPL string type.
 */
template <typename T>
struct TypeBuilder<T, std::enable_if_t<std::is_convertible_v<T, std::string_view>>> {
  static ast::Type *Get(Context *context) { return ast::StringType::Get(context); }
};

/**
 * Specialize for functions.
 */
template <typename Ret, typename... Args>
struct TypeBuilder<Ret(Args...)> {
  static ast::Type *Get(Context *context) {
    auto ret_type = TypeBuilder<Ret>::Get(context);
    if (sizeof...(Args) == 0) {
      return FunctionType::Get(ret_type);
    }
  }
};

}  // namespace tpl::ast
