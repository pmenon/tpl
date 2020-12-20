#pragma once

#include "ast/type.h"

namespace tpl::ast::x {

/**
 * This class contains a list of all builtin types as usable proxies. When dealing with templated
 * types, using a proxy avoids including headers for the full types since, typically, the full type
 * isn't needed during AST construction or code generation.
 */

#define F(kind, ...) struct kind;
BUILTIN_TYPE_LIST(IGNORE_BUILTIN_TYPE, F, F)
#undef F

}  // namespace tpl::ast::x
