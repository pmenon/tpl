#pragma once

#include "ast/context.h"
#include "ast/type.h"
#include "ast/type_proxy.h"

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
 * Specialize for primitive builtin types. These are C++ primitive types exposed as TPL primitives.
 */
#define F(kind, ctype, ...)                                                               \
  template <>                                                                             \
  class TypeBuilder<                                                                      \
      std::conditional_t<ast::BuiltinType::kind == ast::BuiltinType::Nil, void, ctype>> { \
   public:                                                                                \
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
  class TypeBuilder<tpl::ast::x::kind> {                        \
   public:                                                      \
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

}  // namespace tpl::ast
