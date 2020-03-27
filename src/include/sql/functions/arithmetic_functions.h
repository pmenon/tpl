#pragma once

#include <cmath>

#include "sql/operators/numeric_binary_operators.h"
#include "sql/operators/numeric_operators.h"
#include "sql/value.h"

namespace tpl::sql {

/**
 * Utility class to handle various arithmetic SQL functions.
 */
class ArithmeticFunctions : public AllStatic {
 public:
  static void Add(Integer *result, const Integer &a, const Integer &b);
  static void Add(Integer *result, const Integer &a, const Integer &b, bool *overflow);
  static void Add(Real *result, const Real &a, const Real &b);
  static void Sub(Integer *result, const Integer &a, const Integer &b);
  static void Sub(Integer *result, const Integer &a, const Integer &b, bool *overflow);
  static void Sub(Real *result, const Real &a, const Real &b);
  static void Mul(Integer *result, const Integer &a, const Integer &b);
  static void Mul(Integer *result, const Integer &a, const Integer &b, bool *overflow);
  static void Mul(Real *result, const Real &a, const Real &b);
  static void IntDiv(Integer *result, const Integer &a, const Integer &b, bool *div_by_zero);
  static void Div(Real *result, const Real &a, const Real &b, bool *div_by_zero);
  static void IntMod(Integer *result, const Integer &a, const Integer &b, bool *div_by_zero);
  static void Mod(Real *result, const Real &a, const Real &b, bool *div_by_zero);

  static void Pi(Real *result);
  static void E(Real *result);
  static void Abs(Integer *result, const Integer &v);
  static void Abs(Real *result, const Real &v);
  static void Sin(Real *result, const Real &v);
  static void Asin(Real *result, const Real &v);
  static void Cos(Real *result, const Real &v);
  static void Acos(Real *result, const Real &v);
  static void Tan(Real *result, const Real &v);
  static void Cot(Real *result, const Real &v);
  static void Atan(Real *result, const Real &v);
  static void Atan2(Real *result, const Real &a, const Real &b);
  static void Cosh(Real *result, const Real &v);
  static void Tanh(Real *result, const Real &v);
  static void Sinh(Real *result, const Real &v);
  static void Sqrt(Real *result, const Real &v);
  static void Cbrt(Real *result, const Real &v);
  static void Exp(Real *result, const Real &v);
  static void Ceil(Real *result, const Real &v);
  static void Floor(Real *result, const Real &v);
  static void Truncate(Real *result, const Real &v);
  static void Ln(Real *result, const Real &v);
  static void Log2(Real *result, const Real &v);
  static void Log10(Real *result, const Real &v);
  static void Sign(Real *result, const Real &v);
  static void Radians(Real *result, const Real &v);
  static void Degrees(Real *result, const Real &v);
  static void Round(Real *result, const Real &v);
  static void RoundUpTo(Real *result, const Real &v, const Integer &scale);
  static void Log(Real *result, const Real &base, const Real &val);
  static void Pow(Real *result, const Real &base, const Real &val);
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// The functions below are inlined in the header for performance. Don't move it
// unless you know what you're doing.

#define UNARY_MATH_EXPENSIVE_HIDE_NULL(OP, RET_TYPE, INPUT_TYPE)               \
  inline void ArithmeticFunctions::OP(RET_TYPE *result, const INPUT_TYPE &v) { \
    using CppType = decltype(v.val);                                           \
    if (v.is_null) {                                                           \
      *result = RET_TYPE::Null();                                              \
      return;                                                                  \
    }                                                                          \
    *result = RET_TYPE(tpl::sql::OP<CppType>{}(v.val));                        \
  }

#define BINARY_MATH_FAST_HIDE_NULL(NAME, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2, OP) \
  inline void ArithmeticFunctions::NAME(RET_TYPE *result, const INPUT_TYPE1 &a,  \
                                        const INPUT_TYPE2 &b) {                  \
    using CppType = decltype(result->val);                                       \
    result->is_null = (a.is_null || b.is_null);                                  \
    result->val = OP<CppType>{}(a.val, b.val);                                   \
  }

#define BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(NAME, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2, OP) \
  inline void ArithmeticFunctions::NAME(RET_TYPE *result, const INPUT_TYPE1 &a,           \
                                        const INPUT_TYPE2 &b, bool *overflow) {           \
    using CppType = decltype(result->val);                                                \
    result->is_null = (a.is_null || b.is_null);                                           \
    *overflow = OP<CppType>{}(a.val, b.val, &result->val);                                \
  }

#define BINARY_MATH_CHECK_ZERO_HIDE_NULL(NAME, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2, OP) \
  inline void ArithmeticFunctions::NAME(RET_TYPE *result, const INPUT_TYPE1 &a,        \
                                        const INPUT_TYPE2 &b, bool *div_by_zero) {     \
    using CppType = decltype(result->val);                                             \
    if (a.is_null || b.is_null || b.val == 0) {                                        \
      *div_by_zero = true;                                                             \
      *result = RET_TYPE::Null();                                                      \
      return;                                                                          \
    }                                                                                  \
    *result = RET_TYPE(OP<CppType>{}(a.val, b.val));                                   \
  }

BINARY_MATH_FAST_HIDE_NULL(Add, Integer, Integer, Integer, tpl::sql::Add);
BINARY_MATH_FAST_HIDE_NULL(Add, Real, Real, Real, tpl::sql::Add);
BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(Add, Integer, Integer, Integer, tpl::sql::AddWithOverflow);
BINARY_MATH_FAST_HIDE_NULL(Sub, Integer, Integer, Integer, tpl::sql::Subtract);
BINARY_MATH_FAST_HIDE_NULL(Sub, Real, Real, Real, tpl::sql::Subtract);
BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(Sub, Integer, Integer, Integer, tpl::sql::SubtractWithOverflow);
BINARY_MATH_FAST_HIDE_NULL(Mul, Integer, Integer, Integer, tpl::sql::Multiply);
BINARY_MATH_FAST_HIDE_NULL(Mul, Real, Real, Real, tpl::sql::Multiply);
BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(Mul, Integer, Integer, Integer, tpl::sql::MultiplyWithOverflow);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(IntDiv, Integer, Integer, Integer, tpl::sql::Divide);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(Div, Real, Real, Real, tpl::sql::Divide);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(IntMod, Integer, Integer, Integer, tpl::sql::Modulo);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(Mod, Real, Real, Real, tpl::sql::Modulo);

inline void ArithmeticFunctions::Pi(Real *result) { *result = Real(M_PI); }

inline void ArithmeticFunctions::E(Real *result) { *result = Real(M_E); }

UNARY_MATH_EXPENSIVE_HIDE_NULL(Abs, Integer, Integer);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Abs, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Sin, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Asin, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Cos, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Acos, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Tan, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Cot, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Atan, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Cosh, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Tanh, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Sinh, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Sqrt, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Cbrt, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Ceil, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Floor, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Truncate, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Ln, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Log2, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Log10, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Exp, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Sign, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Radians, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Degrees, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Round, Real, Real);

inline void ArithmeticFunctions::Atan2(Real *result, const Real &a, const Real &b) {
  if (a.is_null || b.is_null) {
    *result = Real::Null();
    return;
  }
  *result = Real(tpl::sql::Atan2<double>{}(a.val, b.val));
}

inline void ArithmeticFunctions::RoundUpTo(Real *result, const Real &v, const Integer &scale) {
  if (v.is_null || scale.is_null) {
    *result = Real::Null();
    return;
  }
  *result = Real(tpl::sql::RoundUpTo<double, int64_t>{}(v.val, scale.val));
}

inline void ArithmeticFunctions::Log(Real *result, const Real &base, const Real &val) {
  if (base.is_null || val.is_null) {
    *result = Real::Null();
    return;
  }
  *result = Real(tpl::sql::Log<double>{}(base.val, val.val));
}

inline void ArithmeticFunctions::Pow(Real *result, const Real &base, const Real &val) {
  if (base.is_null || val.is_null) {
    *result = Real::Null();
    return;
  }
  *result = Real(tpl::sql::Pow<double, double>{}(base.val, val.val));
}

#undef BINARY_FN_CHECK_ZERO
#undef BINARY_MATH_CHECK_ZERO_HIDE_NULL
#undef BINARY_MATH_FAST_HIDE_NULL_OVERFLOW
#undef BINARY_MATH_FAST_HIDE_NULL
#undef UNARY_MATH_EXPENSIVE_HIDE_NULL

}  // namespace tpl::sql
