#pragma once

#include <cstdint>

#include "util/common.h"

extern "C" {

////////////////////////////////////////////////////////////////////////////////
///
/// Primitive operations
///
////////////////////////////////////////////////////////////////////////////////

/// Comparisons
#define COMPARISONS(type)                                             \
  void OpGreaterThanEqual##_##type(bool *result, type lhs, type rhs); \
  void OpGreaterThan##_##type(bool *result, type lhs, type rhs);      \
  void OpEqual##_##type(bool *result, type lhs, type rhs);            \
  void OpLessThanEqual##_##type(bool *result, type lhs, type rhs);    \
  void OpLessThan##_##type(bool *result, type lhs, type rhs);         \
  void OpNotEqual##_##type(bool *result, type lhs, type rhs);

/// Arithmetic
#define ARITHMETIC(type)                                 \
  void OpAdd##_##type(type *result, type lhs, type rhs); \
  void OpSub##_##type(type *result, type lhs, type rhs); \
  void OpMul##_##type(type *result, type lhs, type rhs); \
  void OpDiv##_##type(type *result, type lhs, type rhs); \
  void OpRem##_##type(type *result, type lhs, type rhs);

#define LOAD_CONSTANT(type) void OpLoadConstant##_##type(type *result, type c);

INT_TYPES(COMPARISONS)
INT_TYPES(ARITHMETIC)
INT_TYPES(LOAD_CONSTANT)

#undef COMPARISONS
#undef ARITHMETIC
#undef LOAD_CONSTANT

}  // extern "C"