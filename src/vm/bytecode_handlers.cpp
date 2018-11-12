#include "vm/bytecode_handlers.h"

#include "logging/logger.h"
#include "sql/catalog.h"
#include "sql/table.h"
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
  }                                                       \
                                                          \
  /* Primitive negation */                                \
  void OpNeg##_##type(type *result, type input) { *result = -input; }

/// Bitwise operations
#define BITS(type)                                           \
  /* Primitive bitwise AND */                                \
  void OpBitAnd##_##type(type *result, type lhs, type rhs) { \
    *result = (lhs & rhs);                                   \
  }                                                          \
                                                             \
  /* Primitive bitwise OR */                                 \
  void OpBitOr##_##type(type *result, type lhs, type rhs) {  \
    *result = (lhs | rhs);                                   \
  }                                                          \
                                                             \
  /* Primitive bitwise XOR */                                \
  void OpBitXor##_##type(type *result, type lhs, type rhs) { \
    *result = (lhs ^ rhs);                                   \
  }                                                          \
                                                             \
  /* Primitive bitwise COMPLEMENT */                         \
  void OpBitNeg##_##type(type *result, type input) { *result = ~input; }

/// Move operations
#define MOVE(type) \
  void OpMove##_##type(type *result, type input) { *result = input; }

#define LOAD_CONSTANT(type) \
  void OpLoadImm##_##type(type *result, type c) { *result = c; }

INT_TYPES(COMPARISONS);
INT_TYPES(ARITHMETIC);
INT_TYPES(BITS);
INT_TYPES(MOVE);
INT_TYPES(LOAD_CONSTANT)

MOVE(bool)

#undef COMPARISONS
#undef ARITHMETIC
#undef BITS
#undef MOVE
#undef LOAD_CONSTANT

void OpDeref1(i8 *dest, i8 *src) { *dest = *src; }

void OpDeref2(i16 *dest, i16 *src) { *dest = *src; }

void OpDeref4(i32 *dest, i32 *src) { *dest = *src; }

void OpDeref8(i64 *dest, i64 *src) { *dest = *src; }

void OpDerefN(byte *dest, byte *src, u32 len) { std::memcpy(dest, src, len); }

void OpLea(byte **dest, byte *base, u32 offset) { *dest = base + offset; }

void OpLeaScaled(byte **dest, byte *base, u32 index, u32 scale, u32 offset) {
  *dest = base + (scale * index) + offset;
}

bool OpJump() { return true; }

bool OpJumpIfTrue(bool cond) { return cond; }

bool OpJumpIfFalse(bool cond) { return !cond; }

////////////////////////////////////////////////////////////////////////////////
///
/// Table
///
////////////////////////////////////////////////////////////////////////////////

void OpSqlTableIteratorInit(tpl::sql::TableIterator *iter, u16 table_id) {
  TPL_ASSERT(iter != nullptr, "Null iterator!");

  auto *table = tpl::sql::Catalog::instance()->LookupTableById(
      static_cast<tpl::sql::TableId>(table_id));

  if (table == nullptr) {
    LOG_ERROR("Table with ID {} does not exist!", table_id);
    throw std::runtime_error("Table does not exist");
  }

  new (iter) tpl::sql::TableIterator(table);
}

void OpSqlTableIteratorNext(bool *has_more, tpl::sql::TableIterator *iter) {
  *has_more = iter->Next();
}

void OpSqlTableIteratorClose(tpl::sql::TableIterator *iter) {
  iter->~TableIterator();
}

void OpReadSmallInt(tpl::sql::TableIterator *iter, u32 col_idx,
                    tpl::sql::Integer *val) {
  iter->GetIntegerColumn<tpl::sql::TypeId::SmallInt, false>(col_idx, val);
}

void OpReadInt(tpl::sql::TableIterator *iter, u32 col_idx,
               tpl::sql::Integer *val) {
  iter->GetIntegerColumn<tpl::sql::TypeId::Integer, false>(col_idx, val);
}

void OpReadBigInt(tpl::sql::TableIterator *iter, u32 col_idx,
                  tpl::sql::Integer *val) {
  iter->GetIntegerColumn<tpl::sql::TypeId::BigInt, false>(col_idx, val);
}

void OpReadDecimal(tpl::sql::TableIterator *iter, u32 col_idx,
                   tpl::sql::Decimal *val) {
  iter->GetDecimalColumn<false>(col_idx, val);
}

void OpForceBoolTruth(bool *result, tpl::sql::Integer *input) {
  *result = input->val.boolean;
}

void OpInitInteger(tpl::sql::Integer *result, i32 input) {
  result->val.integer = input;
  result->null = false;
}

void OpGreaterThanInteger(tpl::sql::Integer *result, tpl::sql::Integer *left,
                          tpl::sql::Integer *right) {
  result->val.boolean = left->val.integer > right->val.integer;
  result->null = left->null || right->null;
}

void OpGreaterThanEqualInteger(tpl::sql::Integer *result,
                               tpl::sql::Integer *left,
                               tpl::sql::Integer *right) {
  result->val.boolean = left->val.integer >= right->val.integer;
  result->null = left->null || right->null;
}

void OpEqualInteger(tpl::sql::Integer *result, tpl::sql::Integer *left,
                    tpl::sql::Integer *right) {
  result->val.boolean = left->val.integer == right->val.integer;
  result->null = left->null || right->null;
}

void OpLessThanInteger(tpl::sql::Integer *result, tpl::sql::Integer *left,
                       tpl::sql::Integer *right) {
  result->val.boolean = left->val.integer < right->val.integer;
  result->null = left->null || right->null;
}

void OpLessThanEqualInteger(tpl::sql::Integer *result, tpl::sql::Integer *left,
                            tpl::sql::Integer *right) {
  result->val.boolean = left->val.integer <= right->val.integer;
  result->null = left->null || right->null;
}

void OpNotEqualInteger(tpl::sql::Integer *result, tpl::sql::Integer *left,
                       tpl::sql::Integer *right) {
  result->val.boolean = left->val.integer != right->val.integer;
  result->null = left->null || right->null;
}
