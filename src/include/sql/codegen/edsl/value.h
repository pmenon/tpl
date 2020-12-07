#pragma once

#include <type_traits>
#include <variant>

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/fwd.h"
#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/function_builder.h"

namespace tpl::sql::codegen::edsl {

template <typename T>
class Value {
 public:
  /**
   * The ETL value type.
   **/
  using ValueType = T;

  /**
   * A value can be constructed from another to create an ALIAS. If the original integer is named,
   * then valid modifications to this value will be reflected in the other, and vice versa. If the
   * value integer is an unnamed (i.e., temporary) integer, both variables are identical. In either
   * case, both the original and copy can be used interchangeably.
   *
   * Example:
   * @code
   * Int8 a(codegen, "a", 0);   // var a: int8 = 0
   * Int8 a_copy(a);            // a_copy is an alias to "a"
   * a = 1;                     // a and a_copy BOTH have value 1
   * a_copy = 2;                // a and a_copy BOTH have value 2
   *
   * Int16 a(codegen, "a", 1);  // var a: int16 = 1
   * Int16 b = (a<<1)/10;       // b has the unnamed result of the expression.
   * Int16 c(b);                // c has the same value as b.
   * a = c;                     // a = (a<<1)/10
   * @endcode
   *
   * @param that The value to copy.
   */
  Value(const Value &that) = default;

  /**
   * A value can be created from another R-Value value. If the other value is named, this value will
   * take on the properties of the previous, i.e., it will be usable by name. If the other value
   * represents a temporary unnamed value, evaluations of this object will produce the intermediate
   * value.
   * @param that The value to move.
   */
  Value(Value &&that) = default;

  /**
   * Defaulted virtual destructor.
   */
  virtual ~Value() = default;

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const { return codegen_; }

  /**
   * @return True if this integer represents an intermediate unnamed value; false otherwise.
   */
  bool IsUnnamed() const noexcept { return std::holds_alternative<Evaluator>(value_); }

  /**
   * @return True if this integer represents an named value; false otherwise.
   */
  bool IsNamed() const noexcept { return !IsUnnamed(); }

  /**
   * Evaluate the value into an AST expression.
   * @param codegen The code generator instance.
   * @return An expression representing the current value.
   */
  [[nodiscard]] ast::Expression *Eval() const {
    if (IsNamed()) {
      return codegen_->MakeExpr(std::get<ast::Identifier>(value_));
    } else {
      return std::get<Evaluator>(value_)();
    }
  }

  /**
   * Assign the given value to this value. If this is a named value, an assignment is generator.
   * If this value is unnamed, it will take on the value of the provided expression.
   * @tparam E The expression type.
   * @param val The value to assign.
   * @return This type as the most specific derived type.
   */
  template <typename E, typename = std::enable_if<IsETLExpr<E>>>
  ValueType &operator=(E &&val) {
    if (IsNamed()) {
      codegen_->GetCurrentFunction()->Append(codegen_->Assign(Eval(), val.Eval()));
    } else {
      value_ = [v = std::move(val)]() { return v.Eval(); };
    };
    return *Derived();
  }

  /**
   * Assign the given value to this value.
   * @param that The value to assign.
   * @return This type as the most specific derived type.
   */
  ValueType &operator=(const Value &that) {
    if (IsNamed()) {
      codegen_->GetCurrentFunction()->Append(codegen_->Assign(Eval(), that.Eval()));
    }
    return *Derived();
  }

 private:
  /** @return The derived type.  */
  ValueType *Derived() { return static_cast<ValueType *>(this); }

  /** @return The derived type. */
  const ValueType *Derived() const { return static_cast<const ValueType *>(this); }

 protected:
  /**
   * Create an unnamed element with the given value. Every evaluation of this value return a new
   * instance of the given value.
   *
   * Protected to force subclass usage.
   *
   * @tparam E The ETL expression type.
   * @param val The value to assign.
   */
  template <typename E,
            typename = std::enable_if_t<IsETLExpr<E> && std::is_same_v<ValueType, ValueT<E>>>>
  Value(E &&val)
      : codegen_(val.GetCodeGen()), value_([v = std::move(val)]() { return v.Eval(); }) {}

  /**
   * Create a named value with the given name and with the provided initial value.
   *
   * Protected to force subclass usage.
   *
   * @tparam E The ETL expression type.
   * @param codegen The code generator instance.
   * @param name The name of the value.
   * @param val The value to assign.
   */
  template <typename E,
            typename = std::enable_if_t<IsETLExpr<E> && std::is_same_v<ValueType, ValueT<E>>>>
  Value(CodeGen *codegen, ast::Identifier name, E &&val) : codegen_(codegen), value_(name) {
    codegen->GetCurrentFunction()->Append(codegen->DeclareVarWithInit(name, val.Eval()));
  }

  /**
   * Create a named value with the given name.
   *
   * Protected to force subclass usage.
   *
   * @tparam E The ETL expression type.
   * @param name The name of the value.
   */
  Value(CodeGen *codegen, ast::Identifier name) : codegen_(codegen), value_(name) {}

  /**
   * Create a named value with the given name. If the name conflicts with one already appearing in
   * the same scope, a new one version is created.
   *
   * Protected to force subclass usage.
   *
   * @tparam E The ETL expression type.
   * @param codegen The code generator instance.
   * @param name The name of the value.
   */
  Value(CodeGen *codegen, std::string_view name)
      : codegen_(codegen), value_(codegen->MakeFreshIdentifier(name)) {}

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The discriminated union.
  using Evaluator = std::function<ast::Expression *()>;
  std::variant<ast::Identifier, Evaluator> value_;
};

}  // namespace tpl::sql::codegen::edsl
