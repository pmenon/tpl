#pragma once

#include "sql/codegen/edsl/traits.h"
#include "sql/codegen/edsl/unary_expression.h"
#include "sql/codegen/edsl/value.h"

namespace tpl::sql::codegen::edsl {

template <typename T>
class Ptr : public Value<Ptr<T>> {
 public:
  /**
   * The base type.
   */
  using Base = Value<Ptr<T>>;

  /**
   * The ETL element type.
   */
  using ValueType = Ptr<T>;

  /**
   * The type pointed to.
   */
  using PointeeType = T;

  /**
   * Bring in base overloads.
   */
  using Base::Eval;
  using Base::GetCodeGen;
  using Base::operator=;

  /**
   * Create a new pointer type.
   * @param codegen The code generator.
   * @param name The name of the value.
   */
  Ptr(CodeGen *codegen, std::string_view name)
      : Base(codegen, codegen->MakeFreshIdentifier(name)) {}

  /**
   * Load the value of the pointer.
   * @return
   */
  auto Load() const {
    return UnaryExpression<PointeeType, Ptr<T>>(parsing::Token::Type::STAR, *this);
  }

  /**
   * Store a value at the address pointed to by this pointer.
   * @tparam E
   */
  template <typename E, typename = std::enable_if_t<IsETLExpr<E> && SameValueType<PointeeType, E>>>
  void Store(E &&val) {
    CodeGen *codegen = GetCodeGen();
    FunctionBuilder *function = codegen->GetCurrentFunction();
    function->Append(codegen->Assign(Load().Eval(), val.Eval()));
  }
};

template <typename T>
struct Traits<Ptr<T>> {
  /** The EDSL value type of the expression. */
  using ValueType = Ptr<T>;
  /** The raw C++ type of the expression. */
  using CppType = T *;
  /** Indicates if T is an ETL type. */
  static constexpr bool kIsETL = true;
};

}  // namespace tpl::sql::codegen::edsl
