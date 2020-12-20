#pragma once

#include <type_traits>

#include "ast/type.h"
#include "ast/type_proxy.h"
#include "common/common.h"

namespace tpl::sql::codegen::edsl::traits {

/**
 * Type trait to determine if the given C++ type is a builtin TPL type.
 * @tparam T The C++ type to check.
 */
template <typename T>
struct is_builtin_type : std::false_type {};

#define F(kind, cpp_type, ...)                                                             \
  template <>                                                                              \
  struct is_builtin_type<                                                                  \
      std::conditional_t<ast::BuiltinType::kind == ast::BuiltinType::Nil, void, cpp_type>> \
      : std::true_type {};
PRIMITIVE_BUILTIN_TYPE_LIST(F)
#undef F

/**
 * Specialize for non-primitive TPL types. Non-primitive TPL types are TPL builtins, too.
 * But, we use their proxy type instead of the raw C++ type. This reduces header includes.
 */
// clang-format off
#define F(kind, ...) template <> struct is_builtin_type<tpl::ast::x::kind> : std::true_type {};
NON_PRIMITIVE_BUILTIN_TYPE_LIST(F)
SQL_BUILTIN_TYPE_LIST(F)
#undef F
// clang-format on

/**
 * Type trait to determine if the given C++ type is a C++ primitive.
 * @tparam T The C++ type to check.
 */
template <typename T>
struct is_primitive_type : std::false_type {};
// clang-format off
#define F(cpp_type, ...) template <> struct is_primitive_type<cpp_type> : std::true_type {};
// clang-format on
ALL_NUMERIC_TYPES(F)
#undef F

/**
 * Type trait to determine if the given type is a primitive C++ integer type.
 * @tparam T The type to check.
 */
template <typename T>
struct is_primitive_int_type : std::false_type {};
// clang-format off
#define F(type, ...) template <> struct is_primitive_int_type<type> : std::true_type {};
// clang-format on
FOR_EACH_SIGNED_INT_TYPE(F)
FOR_EACH_UNSIGNED_INT_TYPE(F)
#undef F

/**
 * Type trait to determine if the given type is a primitive C++ floating-point type.
 * @tparam T The type to check.
 */
template <typename T>
struct is_primitive_float_type : std::false_type {};
// clang-format off
#define F(type, ...) template <> struct is_primitive_float_type<type> : std::true_type {};
// clang-format on
FOR_EACH_FLOAT_TYPE(F)
#undef F

/**
 * Type trait to determine if the given type is a builtin TPL class type. These can have
 * functions invoked on them.
 * @tparam T The type to check.
 */
template <typename T>
struct is_cpp_class : std::false_type {};
// clang-format off
#define F(kind, ...) template <> struct is_cpp_class<ast::x::kind> : std::true_type {};
// clang-format on
BUILTIN_TYPE_LIST(IGNORE_BUILTIN_TYPE, F, F)
#undef F

/**
 * Type trait to determine if the given C++ type is a SQL value type. This only includes any of
 * the *Val types that represent NULL-able SQL values.
 * @tparam T The C++ type to check.
 */
template <typename T>
struct is_sql_value_class : std::false_type {};
// clang-format off
#define F(kind, ...) template <> struct is_sql_value_class<ast::x::kind> : std::true_type {};
// clang-format on
BUILTIN_TYPE_LIST(IGNORE_BUILTIN_TYPE, IGNORE_BUILTIN_TYPE, F)
#undef F

/**
 * Trait to check if the template type supports arithmetic addition, subtraction or multiplication.
 * @tparam T The C++ type to check.
 */
template <typename T>
struct supports_addsubmul
    : std::integral_constant<bool, is_primitive_type<T>::value && !std::is_same_v<T, bool>> {};

/**
 * Trait to check if the template type supports arithmetic division.
 * @tparam T The C++ type to check.
 */
template <typename T>
struct supports_div
    : std::integral_constant<bool, is_primitive_type<T>::value && !std::is_same_v<T, bool>> {};

/**
 * Trait to check if the template type supports arithmetic division.
 * @tparam T The C++ type to check.
 */
template <typename T>
struct supports_modulo
    : std::integral_constant<bool, is_primitive_int_type<T>::value && !std::is_same_v<T, bool>> {};

/**
 * Trait to check if the template type supports equality comparisons.
 * @tparam T The C++ type to check.
 */
template <typename T>
struct supports_equal : std::integral_constant<bool, is_primitive_type<T>::value> {};

/**
 * Trait to check if the template type supports greater-than comparisons.
 * @tparam T The C++ type to check.
 */
template <typename T>
struct supports_greater
    : std::integral_constant<bool, is_primitive_type<T>::value && !std::is_same_v<T, bool>> {};

/**
 * Helper variable for is_builtin_type<T>::value.
 * @tparam T The C++ type to check.
 */
template <typename T>
inline constexpr bool is_builtin_type_v = is_builtin_type<T>::value;

/**
 * Helper variable for is_cpp_class_v<T>::value.
 * @tparam T The C++ type to check.
 */
template <typename T>
inline constexpr bool is_cpp_class_v = is_cpp_class<T>::value;

/**
 * Helper variable for is_sql_value_class<T>::value.
 * @tparam T The C++ type to check.
 */
template <typename T>
inline constexpr bool is_sql_value_class_v = is_sql_value_class<T>::value;

template <typename T>
concept TPLType = is_builtin_type_v<T> || std::is_pointer_v<T> || std::is_array_v<T>;

}  // namespace tpl::sql::codegen::edsl::traits
