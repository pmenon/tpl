#pragma once

#include <cmath>

#include "sql/operations/numeric_binary_operators.h"
#include "sql/operations/numeric_operators.h"
#include "sql/value.h"

namespace tpl::sql {

/**
 * Utility class to handle various arithmetic SQL functions.
 */
class ArithmeticFunctions {
 public:
  // Delete to force only static functions
  ArithmeticFunctions() = delete;

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
    if (v.is_null) {                                                           \
      *result = RET_TYPE::Null();                                              \
      return;                                                                  \
    }                                                                          \
    *result = RET_TYPE(OP::Apply(v.val));                                      \
  }

#define BINARY_MATH_EXPENSIVE_HIDE_NULL(OP, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2) \
  inline void ArithmeticFunctions::OP(RET_TYPE *result, const INPUT_TYPE1 &a,   \
                                      const INPUT_TYPE2 &b) {                   \
    if (a.is_null || b.is_null) {                                               \
      *result = RET_TYPE::Null();                                               \
      return;                                                                   \
    }                                                                           \
    *result = RET_TYPE(OP::Apply(a.val, b.val));                                \
  }

#define BINARY_MATH_FAST_HIDE_NULL(NAME, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2, OP) \
  inline void ArithmeticFunctions::NAME(RET_TYPE *result, const INPUT_TYPE1 &a,  \
                                        const INPUT_TYPE2 &b) {                  \
    result->is_null = (a.is_null || b.is_null);                                  \
    result->val = OP::Apply(a.val, b.val);                                       \
  }

#define BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(NAME, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2, OP) \
  inline void ArithmeticFunctions::NAME(RET_TYPE *result, const INPUT_TYPE1 &a,           \
                                        const INPUT_TYPE2 &b, bool *overflow) {           \
    result->is_null = (a.is_null || b.is_null);                                           \
    *overflow = OP::Apply(a.val, b.val, &result->val);                                    \
  }

#define BINARY_MATH_CHECK_ZERO_HIDE_NULL(NAME, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2, OP) \
  inline void ArithmeticFunctions::NAME(RET_TYPE *result, const INPUT_TYPE1 &a,        \
                                        const INPUT_TYPE2 &b, bool *div_by_zero) {     \
    if (a.is_null || b.is_null || b.val == 0) {                                        \
      *div_by_zero = true;                                                             \
      *result = RET_TYPE::Null();                                                      \
      return;                                                                          \
    }                                                                                  \
    *result = RET_TYPE(OP::Apply(a.val, b.val));                                       \
  }

BINARY_MATH_FAST_HIDE_NULL(Add, Integer, Integer, Integer, Add);
BINARY_MATH_FAST_HIDE_NULL(Add, Real, Real, Real, Add);
BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(Add, Integer, Integer, Integer, Add);
BINARY_MATH_FAST_HIDE_NULL(Sub, Integer, Integer, Integer, Subtract);
BINARY_MATH_FAST_HIDE_NULL(Sub, Real, Real, Real, Subtract);
BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(Sub, Integer, Integer, Integer, Subtract);
BINARY_MATH_FAST_HIDE_NULL(Mul, Integer, Integer, Integer, Multiply);
BINARY_MATH_FAST_HIDE_NULL(Mul, Real, Real, Real, Multiply);
BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(Mul, Integer, Integer, Integer, Multiply);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(IntDiv, Integer, Integer, Integer, Divide);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(Div, Real, Real, Real, Divide);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(IntMod, Integer, Integer, Integer, Modulo);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(Mod, Real, Real, Real, Modulo);

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

BINARY_MATH_EXPENSIVE_HIDE_NULL(Atan2, Real, Real, Real);
BINARY_MATH_EXPENSIVE_HIDE_NULL(Log, Real, Real, Real);
BINARY_MATH_EXPENSIVE_HIDE_NULL(Pow, Real, Real, Real);
BINARY_MATH_EXPENSIVE_HIDE_NULL(RoundUpTo, Real, Real, Integer);

#undef BINARY_FN_CHECK_ZERO
#undef BINARY_MATH_CHECK_ZERO_HIDE_NULL
#undef BINARY_MATH_FAST_HIDE_NULL_OVERFLOW
#undef BINARY_MATH_FAST_HIDE_NULL
#undef BINARY_MATH_EXPENSIVE_HIDE_NULL
#undef UNARY_MATH_EXPENSIVE_HIDE_NULL

}  // namespace tpl::sql
