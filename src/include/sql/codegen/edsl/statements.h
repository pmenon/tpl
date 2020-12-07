#pragma once

#include <type_traits>

#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/bool.h"
#include "sql/codegen/loop.h"

namespace tpl::sql::codegen::edsl {

template <typename Condition, typename Body, typename = std::enable_if_t<HasBoolValue<Condition>>>
void while_(CodeGen *codegen, Condition condition, Body body) {
  Loop loop(codegen->GetCurrentFunction(), condition.Eval());
  {
    // Body.
    body();
  }
  loop.EndLoop();
}

}  // namespace tpl::sql::codegen::edsl
