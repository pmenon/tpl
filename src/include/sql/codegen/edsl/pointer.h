#pragma once

#include "sql/codegen/edsl/element_base.h"
#include "sql/codegen/edsl/traits.h"

namespace tpl::sql::codegen::edsl {

#if 0
template <typename T>
class Ptr : public NamedValue {
 public:
  Ptr(CodeGen *codegen, std::string_view name)
      : NamedValue(codegen, codegen->MakeFreshIdentifier(name)) {
    // Evaluate the type representation.
    ast::Expression *type_repr = TypeBuilder<T *>::MakeTypeRepr(codegen);
    // Generate the declaration.
    FunctionBuilder *function = codegen->GetCurrentFunction();
    function->Append(codegen->DeclareVarNoInit(name_, type_repr));
  }
};
#endif

}  // namespace tpl::sql::codegen::edsl
