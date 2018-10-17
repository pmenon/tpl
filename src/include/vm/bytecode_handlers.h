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
  void OpRem##_##type(type *result, type lhs, type rhs); \
  void OpNeg##_##type(type *result, type input);

/// Bitwise operations
#define BITS(type)                                          \
  void OpBitAnd##_##type(type *result, type lhs, type rhs); \
  void OpBitOr##_##type(type *result, type lhs, type rhs);  \
  void OpBitXor##_##type(type *result, type lhs, type rhs); \
  void OpBitNeg##_##type(type *result, type input);

#define LOAD_CONSTANT(type) void OpLoadImm##_##type(type *result, type c);

INT_TYPES(COMPARISONS)
INT_TYPES(ARITHMETIC)
INT_TYPES(BITS)
INT_TYPES(LOAD_CONSTANT)

#undef COMPARISONS
#undef ARITHMETIC
#undef BITS
#undef LOAD_CONSTANT

void OpLea(byte **dest, byte *src, u32 offset);

////////////////////////////////////////////////////////////////////////////////
///
/// Branching operations
///
////////////////////////////////////////////////////////////////////////////////

bool OpJump();
bool OpJumpIfTrue(bool cond);
bool OpJumpIfFalse(bool cond);

}  // extern "C"