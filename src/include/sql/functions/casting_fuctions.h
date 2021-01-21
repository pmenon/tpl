#pragma once

#include <string>

#include "sql/execution_context.h"
#include "sql/functions/helpers.h"
#include "sql/operators/cast_operators.h"
#include "sql/value.h"

namespace tpl::sql {

class ExecutionContext;

/**
 * Utility class to handle various SQL casting functions.
 */
class CastingFunctions : public AllStatic {
 public:
  /**
   * Perform a simple cast from @em input with SQL type 'T' into a SQL value of type 'R'. The cast
   * is performed "blind", meaning it ignores the NULL-ness of the input.
   * @tparam R The type to cast to.
   * @tparam T The type to cast from.
   * @param[out] result Where the result of the cast is stored.
   * @param input The input.
   */
  template <SQLValueType R, SQLValueType T>
  static void CastToSimpleType(R *result, const T &input) {
    using V = decltype(R::val);
    UnaryFunction::EvalFast(result, input, []<typename U>(U in) {
      V out{};
      tpl::sql::TryCast<U, V>{}(in, &out);
      return out;
    });
  }

  /**
   * Perform a cast from the string input @em input into a SQL value of type 'R'.
   * - If the input is NULL, the output is NULL.
   * - If the input is not NULL, a cast is attempted to the target type.
   * @tparam R The type to cast to.
   * @param[out] result Where the result of the cast is stored.
   * @param input The string input to cast input.
   */
  template <SQLValueType R>
  static void CastToSimpleType(R *result, const StringVal &input) {
    using V = decltype(R::val);
    UnaryFunction::EvalHideNull(result, input, []<typename U>(U in) {
      V out{};
      tpl::sql::TryCast<U, V>{}(in, &out);
      return out;
    });
  }

  /**
   * Cast @em input with type 'T' into a string. The execution context is provided for its string
   * pool for memory allocation.
   * @tparam T The input type to cast from.
   * @param[out] result Where the result of the cast is stored.
   * @param ctx The execution context required for memory allocation.
   * @param input The input to the cast.
   */
  template <SQLValueType T>
  static void CastToString(StringVal *result, ExecutionContext *ctx, const T &input) {
    UnaryFunction::EvalHideNull(result, input, [ctx]<typename U>(U in) {
      const auto str = tpl::sql::Cast<U, std::string>{}(in);
      return ctx->GetStringHeap()->AddVarlen(str);
    });
  }
};

}  // namespace tpl::sql
