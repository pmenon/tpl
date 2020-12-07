#pragma once

#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/traits.h"

namespace tpl::sql::codegen::edsl {

/**
 * A very simple, non-generating base class for any named value. A named value is any TPL element
 * that appears in source code with a name - in other words, it has a defined address. This can be
 * variables, function parameters, or struct members. We treat them uniformly in the EDSL.
 */
class NamedValue {
 public:
  CodeGen *GetCodeGen() const { return codegen_; }

 protected:
  // Protected to force sub-class usage.
  NamedValue(CodeGen *codegen, ast::Identifier name) : codegen_(codegen), name_(name) {}

 protected:
  // The code generator.
  CodeGen *codegen_;
  // The name.
  ast::Identifier name_;
};

//template <typename T, typename = void>
//struct has_eval : std::false_type {};
//
//template <typename T>
//struct has_eval<T, decltype((void)std::declval<T>().Eval())> : std::true_type {};
//
//template <typename T>
//auto Evaluate(CodeGen *codegen, const T &val)
//    -> std::enable_if_t<std::is_integral_v<T>, ast::Expression *> {
//  return GetIntegerLiteral(codegen, val);
//}
//
//template <typename T>
//auto Evaluate(CodeGen *, const T &val)
//    -> std::enable_if_t<has_eval<T>::value, ast::Expression *> {
//  return val.Eval();
//}

}  // namespace tpl::sql::codegen::edsl