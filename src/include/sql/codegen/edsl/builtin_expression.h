#pragma once

#include <tuple>

#include "ast/builtins.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/traits.h"

namespace tpl::sql::codegen::edsl {

template <typename T, typename... Args>
class BuiltinExpression {
 public:
  /**
   * The ETL expression value type.
   */
  using ValueType = T;

  /**
   * Create a new builtin operation.
   * @param args The arguments to the builtin.
   */
  BuiltinExpression(ast::Builtin builtin, Args... args)
      : builtin_(builtin), args_(std::forward<Args>(args)...) {
    codegen_ = std::get<0>(args_).GetCodeGen();
  }

  /**
   * @return The result of the application of the arithmetic operation on the input arguments.
   */
  ast::Expression *Eval() const {
    std::vector<ast::Expression *> params;
    std::apply([&](auto &...x) { params = {x.Eval()...}; }, args_);
    return codegen_->CallBuiltin(builtin_, params);
  }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const noexcept { return codegen_; }

 private:
  // The code generator.
  CodeGen *codegen_;
  // The type of builtin.
  ast::Builtin builtin_;
  // The arguments to the call.
  std::tuple<Args...> args_;
};

/**
 * Trait specialization for builtin operations.
 * @tparam Lhs The type of the left-hand input.
 * @tparam Rhs The type of the right-hand input.
 */
template <typename T, typename... Args>
struct Traits<BuiltinExpression<T, Args...>> {
  /** The value type of the expression. */
  using ValueType = T;
  /** Arithmetic is an ETL expression. */
  static constexpr bool kIsETL = true;
};

}  // namespace tpl::sql::codegen::edsl
