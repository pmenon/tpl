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

void OpDeref1(u8 *dest, u8 *src);
void OpDeref2(u16 *dest, u16 *src);
void OpDeref4(u32 *dest, u32 *src);
void OpDeref8(u64 *dest, u64 *src);
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
/// Table
///
////////////////////////////////////////////////////////////////////////////////

void OpSqlTableIteratorInit(tpl::sql::TableIterator *iter, u16 table_id);
void OpSqlTableIteratorNext(bool *has_more, tpl::sql::TableIterator *iter);
void OpSqlTableIteratorClose(tpl::sql::TableIterator *iter);
void OpReadSmallInt(tpl::sql::TableIterator *iter, u32 col_idx,
                    tpl::sql::Integer *val);
void OpReadInt(tpl::sql::TableIterator *iter, u32 col_idx,
               tpl::sql::Integer *val);
void OpReadBigInt(tpl::sql::TableIterator *iter, u32 col_idx,
                  tpl::sql::Integer *val);
void OpReadDecimal(tpl::sql::TableIterator *iter, u32 col_idx,
                   tpl::sql::Decimal *val);

}  // extern "C"