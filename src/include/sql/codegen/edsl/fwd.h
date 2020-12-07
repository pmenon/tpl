#pragma once

#include "ast/type.h"

namespace tpl::sql::codegen::edsl {

template <typename T>
class Value;

#define F(Name, ...) class Name;
BUILTIN_TYPE_LIST(F, F, F)
#undef F

}  // namespace tpl::sql::codegen::edsl
