#pragma once

#include <type_traits>

#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/bool.h"
#include "sql/codegen/loop.h"

namespace tpl::sql::codegen::edsl {

template <typename Body>
void while_(CodeGen *codegen, const Bool &condition, Body body) {
  Loop loop(codegen->GetCurrentFunction(), condition.Eval());
  {
    // Body.
    body();
  }
  loop.EndLoop();
}

}  // namespace tpl::sql::codegen::edsl
