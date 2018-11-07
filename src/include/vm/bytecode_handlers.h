#pragma once

#include <cstdint>

#include "util/common.h"

namespace tpl::sql {
struct Decimal;
struct Integer;
class TableIterator;
}  // namespace tpl::sql

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

/// Move operations
#define MOVE(type)                                          \
  void OpMove##_##type(type *result, type input);

#define LOAD_CONSTANT(type) void OpLoadImm##_##type(type *result, type c);

INT_TYPES(COMPARISONS)
INT_TYPES(ARITHMETIC)
INT_TYPES(BITS)
INT_TYPES(MOVE)
INT_TYPES(LOAD_CONSTANT)

MOVE(bool)

#undef COMPARISONS
#undef ARITHMETIC
#undef BITS
#undef MOVE
#undef LOAD_CONSTANT

void OpDeref1(i8 *dest, i8 *src);
void OpDeref2(i16 *dest, i16 *src);
void OpDeref4(i32 *dest, i32 *src);
void OpDeref8(i64 *dest, i64 *src);
void OpDerefN(byte *dest, byte *src, u32 len);
void OpLea(byte **dest, byte *src, u32 offset);

////////////////////////////////////////////////////////////////////////////////
///
/// Branching operations
///
////////////////////////////////////////////////////////////////////////////////

bool OpJump();
bool OpJumpIfTrue(bool cond);
bool OpJumpIfFalse(bool cond);

////////////////////////////////////////////////////////////////////////////////
///
/// SQL
///
////////////////////////////////////////////////////////////////////////////////

// Iteration
void OpSqlTableIteratorInit(tpl::sql::TableIterator *iter, u16 table_id);
void OpSqlTableIteratorNext(bool *has_more, tpl::sql::TableIterator *iter);
void OpSqlTableIteratorClose(tpl::sql::TableIterator *iter);

// Reading
void OpReadSmallInt(tpl::sql::TableIterator *iter, u32 col_idx,
                    tpl::sql::Integer *val);
void OpReadInt(tpl::sql::TableIterator *iter, u32 col_idx,
               tpl::sql::Integer *val);
void OpReadBigInt(tpl::sql::TableIterator *iter, u32 col_idx,
                  tpl::sql::Integer *val);
void OpReadDecimal(tpl::sql::TableIterator *iter, u32 col_idx,
                   tpl::sql::Decimal *val);

// SQL Boolean to boolean
void OpForceBoolTruth(bool *result, tpl::sql::Integer *input);

// Native integer to SQL
void OpInitInteger(tpl::sql::Integer *result, i32 input);

void OpGreaterThanInteger(tpl::sql::Integer *result, tpl::sql::Integer *left,
                          tpl::sql::Integer *right);
void OpGreaterThanEqualInteger(tpl::sql::Integer *result,
                               tpl::sql::Integer *left,
                               tpl::sql::Integer *right);
void OpEqualInteger(tpl::sql::Integer *result, tpl::sql::Integer *left,
                    tpl::sql::Integer *right);
void OpLessThanInteger(tpl::sql::Integer *result, tpl::sql::Integer *left,
                       tpl::sql::Integer *right);
void OpLessThanEqualInteger(tpl::sql::Integer *result, tpl::sql::Integer *left,
                            tpl::sql::Integer *right);
void OpNotEqualInteger(tpl::sql::Integer *result, tpl::sql::Integer *left,
                       tpl::sql::Integer *right);

}  // extern "C"