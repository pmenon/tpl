#include "vm/bytecode_handlers.h"

#include "util/macros.h"

/// Comparisons
#define COMPARISONS(type)                                              \
  /* Primitive greater-than-equal implementation */                    \
  void OpGreaterThanEqual##_##type(bool *result, type lhs, type rhs) { \
    *result = (lhs >= rhs);                                            \
  }                                                                    \
                                                                       \
  /* Primitive greater-than implementation */                          \
  void OpGreaterThan##_##type(bool *result, type lhs, type rhs) {      \
    *result = (lhs > rhs);                                             \
  }                                                                    \
                                                                       \
  /* Primitive equal-to implementation */                              \
  void OpEqual##_##type(bool *result, type lhs, type rhs) {            \
    *result = (lhs == rhs);                                            \
  }                                                                    \
                                                                       \
  /* Primitive less-than-equal implementation */                       \
  void OpLessThanEqual##_##type(bool *result, type lhs, type rhs) {    \
    *result = (lhs <= rhs);                                            \
  }                                                                    \
                                                                       \
  /* Primitive less-than implementation */                             \
  void OpLessThan##_##type(bool *result, type lhs, type rhs) {         \
    *result = (lhs < rhs);                                             \
  }                                                                    \
                                                                       \
  /* Primitive not-equal-to implementation */                          \
  void OpNotEqual##_##type(bool *result, type lhs, type rhs) {         \
    *result = (lhs != rhs);                                            \
  }

/// Arithmetic
#define ARITHMETIC(type)                                  \
  /* Primitive addition */                                \
  void OpAdd##_##type(type *result, type lhs, type rhs) { \
    *result = (lhs + rhs);                                \
  }                                                       \
                                                          \
  /* Primitive subtraction */                             \
  void OpSub##_##type(type *result, type lhs, type rhs) { \
    *result = (lhs - rhs);                                \
  }                                                       \
                                                          \
  /* Primitive multiplication */                          \
  void OpMul##_##type(type *result, type lhs, type rhs) { \
    *result = (lhs * rhs);                                \
  }                                                       \
                                                          \
  /* Primitive division (no zero-check) */                \
  void OpDiv##_##type(type *result, type lhs, type rhs) { \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");      \
    *result = (lhs / rhs);                                \
  }                                                       \
                                                          \
  /* Primitive modulo-remainder (no zero-check) */        \
  void OpRem##_##type(type *result, type lhs, type rhs) { \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");      \
    *result = (lhs % rhs);                                \
  }

#define LOAD_CONSTANT(type) \
  void OpLoadConstant##_##type(type *result, type c) { *result = c; }

INT_TYPES(COMPARISONS);
INT_TYPES(ARITHMETIC);
INT_TYPES(LOAD_CONSTANT)