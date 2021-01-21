#include "sql/functions/arithmetic_functions.h"

namespace tpl::sql {

void ArithmeticFunctions::Asin(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(
      result, input, tpl::sql::Asin<decltype(Real::val)>{}, []<typename U>(U in) {
        if (in < U{-1} || in > U{1})
          throw Exception(ExceptionType::Execution, "asin() is undefined outside [-1,1]");
        return true;
      });
}

void ArithmeticFunctions::Acos(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(
      result, input, tpl::sql::Acos<decltype(Real::val)>{}, []<typename U>(U in) {
        if (in < U{-1} || in > U{1})
          throw Exception(ExceptionType::Execution, "acos() is undefined outside [-1,1]");
        return true;
      });
}

void ArithmeticFunctions::Atan(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Atan<decltype(Real::val)>{});
}

void ArithmeticFunctions::Atan2(Real *result, const Real &a, const Real &b) {
  BinaryFunction::EvalHideNull(result, a, b, tpl::sql::Atan2<double>{});
}

void ArithmeticFunctions::Cbrt(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Cbrt<double>{});
}

void ArithmeticFunctions::Ceil(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Ceil<double>{});
}

void ArithmeticFunctions::Cos(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Cos<double>{});
}

void ArithmeticFunctions::Cosh(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Cosh<double>{});
}

void ArithmeticFunctions::Cot(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Cot<double>{});
}

void ArithmeticFunctions::Degrees(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Degrees<double>{});
}

void ArithmeticFunctions::Exp(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Exp<double>{});
}

void ArithmeticFunctions::Floor(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Floor<double>{});
}

void ArithmeticFunctions::Log(Real *result, const Real &base, const Real &val) {
  BinaryFunction::EvalHideNull(result, base, val, tpl::sql::Log<decltype(Real::val)>{});
}

void ArithmeticFunctions::Log2(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Log2<double>{});
}

void ArithmeticFunctions::Log10(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Log10<double>{});
}

void ArithmeticFunctions::Ln(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Ln<double>{});
}

void ArithmeticFunctions::Pow(Real *result, const Real &base, const Real &val) {
  using ValType = decltype(Real::val);
  BinaryFunction::EvalHideNull(result, base, val, tpl::sql::Pow<ValType, ValType>{});
}

void ArithmeticFunctions::Radians(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Radians<double>{});
}

void ArithmeticFunctions::Sign(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Sign<double>{});
}

void ArithmeticFunctions::Sin(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Sin<double>{});
}

void ArithmeticFunctions::Sinh(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Sinh<double>{});
}

void ArithmeticFunctions::Sqrt(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Sqrt<double>{});
}

void ArithmeticFunctions::Tan(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Tan<double>{});
}

void ArithmeticFunctions::Tanh(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Tanh<double>{});
}

void ArithmeticFunctions::Truncate(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Truncate<double>{});
}

void ArithmeticFunctions::Round(Real *result, const Real &input) {
  UnaryFunction::EvalHideNull(result, input, tpl::sql::Round<double>{});
}

void ArithmeticFunctions::RoundUpTo(Real *result, const Real &input, const Integer &scale) {
  using ValType = decltype(Real::val);
  using ScaleType = decltype(Integer::val);
  BinaryFunction::EvalHideNull(result, input, scale, tpl::sql::RoundUpTo<ValType, ScaleType>{});
}

}  // namespace tpl::sql
