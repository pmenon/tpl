#pragma once

#include "ast/type.h"

namespace tpl::sql::codegen::edsl {

/**
 * Base raw untyped value.
 */
class RawValue;

/**
 * Base typed value.
 * @tparam T The EDSL type of the value.
 */
template <typename T>
class Value;

/**
 * Literal values.
 * @tparam T The EDSL type of the value.
 * @tparam Enable Enabler type.
 */
template <typename T, typename Enable = void>
class Literal;

#define F(Name, ...) class Name;
BUILTIN_TYPE_LIST(F, F, F)
#undef F

/**
 * Pointer type.
 * @tparam T The EDSL type of the thing being pointed to.
 */
template <typename T>
class Ptr;

}  // namespace tpl::sql::codegen::edsl
