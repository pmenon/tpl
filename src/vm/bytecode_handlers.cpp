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
#define MOVE(type)  \
  void OpMove##_##type(type *result, type input) { *result = input; }

#define LOAD_CONSTANT(type) \
  void OpLoadImm##_##type(type *result, type c) { *result = c; }

INT_TYPES(COMPARISONS);
INT_TYPES(ARITHMETIC);
INT_TYPES(BITS);
INT_TYPES(MOVE);
INT_TYPES(LOAD_CONSTANT)

#undef COMPARISONS
#undef ARITHMETIC
#undef BITS
#undef MOVE
#undef LOAD_CONSTANT

// TODO(siva); Remove this later
void OpMove_bool(bool *result, bool input) { *result = input; }

void OpDeref1(u8 *dest, u8 *src) { *dest = *src; }

void OpDeref2(u16 *dest, u16 *src) { *dest = *src; }

void OpDeref4(u32 *dest, u32 *src) { *dest = *src; }

void OpDeref8(u64 *dest, u64 *src) { *dest = *src; }

void OpLea(byte **dest, byte *src, u32 offset) { *dest = src + offset; }

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
