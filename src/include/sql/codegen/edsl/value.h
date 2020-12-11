#pragma once

#include <type_traits>

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/fwd.h"
#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/edsl/type_builder.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen::edsl {

/**
 * Base value. These are just fancy names that are untyped.
 */
class ValueBase {
 public:
  /**
   * A value can be constructed from another to create an ALIAS. This means that valid modifications
   * to THIS value will be reflected in the other, and vice versa.
   *
   * Example:
   * @code
   * Int8 a(codegen, "a", 0);   // var a: int8 = 0
   * Int8 a_copy(a);            // a_copy is an alias to "a"
   * a = 1;                     // a and a_copy BOTH have value 1
   * a_copy = 2;                // a and a_copy BOTH have value 2
   * @endcode
   *
   * @param that The value to copy.
   */
  ValueBase(const ValueBase &that) = default;

  /**
   * A value can be created from another R-Value value. The C++ lifetime of the other value ends
   * after this call returns, but the TPL value persists. This means that valid modifications
   * to THIS value will be reflected in the TPL value in generated source.
   * @param that The value to move.
   */
  ValueBase(ValueBase &&that) = default;

  /**
   * Evaluate the value into an AST expression.
   * @param codegen The code generator instance.
   * @return An expression representing the current value.
   */
  ast::Expression *Eval() const { return codegen_->MakeExpr(name_); }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const { return codegen_; }

  /**
   * @return If this value is equivalent to the provided value.
   */
  bool operator==(const ValueBase &that) const noexcept = default;

 protected:
  ValueBase(CodeGen *codegen, ast::Identifier name) : codegen_(codegen), name_(name) {}

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The name.
  ast::Identifier name_;
};

template <typename T>
class Value : public ValueBase {
 public:
  /**
   * The ETL value type.
   **/
  using ValueType = T;

  /**
   * Defaulted virtual destructor.
   */
  virtual ~Value() = default;

  /**
   * Assign the given value to this value.
   * @tparam E The expression type.
   * @param val The value to assign.
   * @return This type as the most specific derived type.
   */
  template <typename E,
            typename = std::enable_if<IsETLExpr<E> && std::is_same_v<ValueType, ValueT<E>>>>
  ValueType &operator=(E &&val) {
    GetCodeGen()->GetCurrentFunction()->Append(GetCodeGen()->Assign(Eval(), val.Eval()));
    return *Derived();
  }

  /**
   * Assign the given value to this value.
   * @param that The value to assign.
   * @return This type as the most specific derived type.
   */
  ValueType &operator=(const Value &that) {
    GetCodeGen()->GetCurrentFunction()->Append(GetCodeGen()->Assign(Eval(), that.Eval()));
    return *Derived();
  }

 private:
  /** @return The derived type.  */
  ValueType *Derived() { return static_cast<ValueType *>(this); }

  /** @return The derived type. */
  const ValueType *Derived() const { return static_cast<const ValueType *>(this); }

 protected:
  /**
   * Create a named value with the given name and initial value.
   * @tparam E The ETL expression type.
   * @param codegen The code generator instance.
   * @param name The name of the value.
   * @param val The value to assign.
   */
  template <typename E,
            typename = std::enable_if_t<IsETLExpr<E> && std::is_same_v<ValueType, ValueT<E>>>>
  Value(CodeGen *codegen, ast::Identifier name, E &&val) : ValueBase(codegen, name) {
    auto type_repr = TypeBuilder<ValueType>::MakeTypeRepr(codegen);
    codegen->GetCurrentFunction()->Append(codegen->DeclareVar(name, type_repr, val.Eval()));
  }

  /**
   * Create a named value with the given name, but with no initial value.
   * @tparam E The ETL expression type.
   * @param name The name of the value.
   */
  Value(CodeGen *codegen, ast::Identifier name) : ValueBase(codegen, name) {
    auto type_repr = TypeBuilder<ValueType>::MakeTypeRepr(codegen);
    codegen->GetCurrentFunction()->Append(codegen->DeclareVarNoInit(name, type_repr));
  }

  /**
   * Create a named value with the given name. If the name conflicts with one already appearing in
   * the same scope, a new one version is created.
   * @tparam E The ETL expression type.
   * @param codegen The code generator instance.
   * @param name The name of the value.
   */
  Value(CodeGen *codegen, std::string_view name)
      : Value(codegen, codegen->MakeFreshIdentifier(name)) {}
};

}  // namespace tpl::sql::codegen::edsl
