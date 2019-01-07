#pragma once

#include <cstdint>

#include "util/common.h"

#include "sql/table_iterator.h"
#include "util/macros.h"

// All VM bytecode op handlers must use this macro
#define VM_OP

// VM bytecodes that are hot and should be inlined should use this macro
#define VM_OP_HOT VM_OP ALWAYS_INLINE inline
#define VM_OP_WARM VM_OP inline
#define VM_OP_COLD VM_OP NEVER_INLINE

extern "C" {

// ---------------------------------------------------------
// Primitive comparisons
// ---------------------------------------------------------

#define COMPARISONS(type)                                                     \
  /* Primitive greater-than-equal implementation */                           \
  VM_OP_HOT void OpGreaterThanEqual##_##type(bool *result, type lhs,          \
                                             type rhs) {                      \
    *result = (lhs >= rhs);                                                   \
  }                                                                           \
                                                                              \
  /* Primitive greater-than implementation */                                 \
  VM_OP_HOT void OpGreaterThan##_##type(bool *result, type lhs, type rhs) {   \
    *result = (lhs > rhs);                                                    \
  }                                                                           \
                                                                              \
  /* Primitive equal-to implementation */                                     \
  VM_OP_HOT void OpEqual##_##type(bool *result, type lhs, type rhs) {         \
    *result = (lhs == rhs);                                                   \
  }                                                                           \
                                                                              \
  /* Primitive less-than-equal implementation */                              \
  VM_OP_HOT void OpLessThanEqual##_##type(bool *result, type lhs, type rhs) { \
    *result = (lhs <= rhs);                                                   \
  }                                                                           \
                                                                              \
  /* Primitive less-than implementation */                                    \
  VM_OP_HOT void OpLessThan##_##type(bool *result, type lhs, type rhs) {      \
    *result = (lhs < rhs);                                                    \
  }                                                                           \
                                                                              \
  /* Primitive not-equal-to implementation */                                 \
  VM_OP_HOT void OpNotEqual##_##type(bool *result, type lhs, type rhs) {      \
    *result = (lhs != rhs);                                                   \
  }

INT_TYPES(COMPARISONS);

#undef COMPARISONS

// ---------------------------------------------------------
// Primitive arithmetic
// ---------------------------------------------------------

#define ARITHMETIC(type)                                            \
  /* Primitive addition */                                          \
  VM_OP_HOT void OpAdd##_##type(type *result, type lhs, type rhs) { \
    *result = (lhs + rhs);                                          \
  }                                                                 \
                                                                    \
  /* Primitive subtraction */                                       \
  VM_OP_HOT void OpSub##_##type(type *result, type lhs, type rhs) { \
    *result = (lhs - rhs);                                          \
  }                                                                 \
                                                                    \
  /* Primitive multiplication */                                    \
  VM_OP_HOT void OpMul##_##type(type *result, type lhs, type rhs) { \
    *result = (lhs * rhs);                                          \
  }                                                                 \
                                                                    \
  /* Primitive division (no zero-check) */                          \
  VM_OP_HOT void OpDiv##_##type(type *result, type lhs, type rhs) { \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");                \
    *result = (lhs / rhs);                                          \
  }                                                                 \
                                                                    \
  /* Primitive modulo-remainder (no zero-check) */                  \
  VM_OP_HOT void OpRem##_##type(type *result, type lhs, type rhs) { \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");                \
    *result = (lhs % rhs);                                          \
  }                                                                 \
                                                                    \
  /* Primitive negation */                                          \
  VM_OP_HOT void OpNeg##_##type(type *result, type input) { *result = -input; }

INT_TYPES(ARITHMETIC);

#undef ARITHMETIC

// ---------------------------------------------------------
// Bitwise operations
// ---------------------------------------------------------

#define BITS(type)                                                     \
  /* Primitive bitwise AND */                                          \
  VM_OP_HOT void OpBitAnd##_##type(type *result, type lhs, type rhs) { \
    *result = (lhs & rhs);                                             \
  }                                                                    \
                                                                       \
  /* Primitive bitwise OR */                                           \
  VM_OP_HOT void OpBitOr##_##type(type *result, type lhs, type rhs) {  \
    *result = (lhs | rhs);                                             \
  }                                                                    \
                                                                       \
  /* Primitive bitwise XOR */                                          \
  VM_OP_HOT void OpBitXor##_##type(type *result, type lhs, type rhs) { \
    *result = (lhs ^ rhs);                                             \
  }                                                                    \
                                                                       \
  /* Primitive bitwise COMPLEMENT */                                   \
  VM_OP_HOT void OpBitNeg##_##type(type *result, type input) {         \
    *result = ~input;                                                  \
  }

INT_TYPES(BITS);

#undef BITS

// ---------------------------------------------------------
// Memory operations
// ---------------------------------------------------------

VM_OP_HOT void OpDeref1(i8 *dest, i8 *src) { *dest = *src; }

VM_OP_HOT void OpDeref2(i16 *dest, i16 *src) { *dest = *src; }

VM_OP_HOT void OpDeref4(i32 *dest, i32 *src) { *dest = *src; }

VM_OP_HOT void OpDeref8(i64 *dest, i64 *src) { *dest = *src; }

VM_OP_HOT void OpDerefN(byte *dest, byte *src, u32 len) {
  TPL_MEMCPY(dest, src, len);
}

VM_OP_HOT void OpAssign1(i8 *dest, i8 src) { *dest = src; }

VM_OP_HOT void OpAssign2(i16 *dest, i16 src) { *dest = src; }

VM_OP_HOT void OpAssign4(i32 *dest, i32 src) { *dest = src; }

VM_OP_HOT void OpAssign8(i64 *dest, i64 src) { *dest = src; }

VM_OP_HOT void OpAssignImm1(i8 *dest, i8 src) { *dest = src; }

VM_OP_HOT void OpAssignImm2(i16 *dest, i16 src) { *dest = src; }

VM_OP_HOT void OpAssignImm4(i32 *dest, i32 src) { *dest = src; }

VM_OP_HOT void OpAssignImm8(i64 *dest, i64 src) { *dest = src; }

VM_OP_HOT void OpLea(byte **dest, byte *base, u32 offset) {
  *dest = base + offset;
}

VM_OP_HOT void OpLeaScaled(byte **dest, byte *base, u32 index, u32 scale,
                           u32 offset) {
  *dest = base + (scale * index) + offset;
}

VM_OP_HOT bool OpJump() { return true; }

VM_OP_HOT bool OpJumpLoop() { return true; }

VM_OP_HOT bool OpJumpIfTrue(bool cond) { return cond; }

VM_OP_HOT bool OpJumpIfFalse(bool cond) { return !cond; }

VM_OP_HOT void OpCall(UNUSED u16 func_id, UNUSED u16 num_args) {}

VM_OP_HOT void OpReturn() {}

// ---------------------------------------------------------
// Table
// ---------------------------------------------------------

VM_OP_COLD void OpSqlTableIteratorInit(tpl::sql::TableIterator *iter,
                                       u16 table_id);

VM_OP_HOT void OpSqlTableIteratorNext(bool *has_more,
                                      tpl::sql::TableIterator *iter) {
  *has_more = iter->Next();
}

VM_OP_COLD void OpSqlTableIteratorClose(tpl::sql::TableIterator *iter);

VM_OP_HOT void OpReadSmallInt(tpl::sql::TableIterator *iter, u32 col_idx,
                              tpl::sql::Integer *val) {
  iter->ReadIntegerColumn<tpl::sql::TypeId::SmallInt, false>(col_idx, val);
}

VM_OP_HOT void OpReadInteger(tpl::sql::TableIterator *iter, u32 col_idx,
                             tpl::sql::Integer *val) {
  iter->ReadIntegerColumn<tpl::sql::TypeId::Integer, false>(col_idx, val);
}

VM_OP_HOT void OpReadBigInt(tpl::sql::TableIterator *iter, u32 col_idx,
                            tpl::sql::Integer *val) {
  iter->ReadIntegerColumn<tpl::sql::TypeId::BigInt, false>(col_idx, val);
}

VM_OP_HOT void OpReadDecimal(tpl::sql::TableIterator *iter, u32 col_idx,
                             tpl::sql::Decimal *val) {
  iter->ReadDecimalColumn<false>(col_idx, val);
}

VM_OP_HOT void OpForceBoolTruth(bool *result, tpl::sql::Integer *input) {
  *result = input->val.boolean;
}

VM_OP_HOT void OpInitInteger(tpl::sql::Integer *result, i32 input) {
  result->val.integer = input;
  result->null = false;
}

VM_OP_HOT void OpGreaterThanInteger(tpl::sql::Integer *result,
                                    tpl::sql::Integer *left,
                                    tpl::sql::Integer *right) {
  result->val.boolean = (left->val.integer > right->val.integer);
  result->null = (left->null || right->null);
}

VM_OP_HOT void OpGreaterThanEqualInteger(tpl::sql::Integer *result,
                                         tpl::sql::Integer *left,
                                         tpl::sql::Integer *right) {
  result->val.boolean = (left->val.integer >= right->val.integer);
  result->null = (left->null || right->null);
}

VM_OP_HOT void OpEqualInteger(tpl::sql::Integer *result,
                              tpl::sql::Integer *left,
                              tpl::sql::Integer *right) {
  result->val.boolean = (left->val.integer == right->val.integer);
  result->null = (left->null || right->null);
}

VM_OP_HOT void OpLessThanInteger(tpl::sql::Integer *result,
                                 tpl::sql::Integer *left,
                                 tpl::sql::Integer *right) {
  result->val.boolean = (left->val.integer < right->val.integer);
  result->null = (left->null || right->null);
}

VM_OP_HOT void OpLessThanEqualInteger(tpl::sql::Integer *result,
                                      tpl::sql::Integer *left,
                                      tpl::sql::Integer *right) {
  result->val.boolean = (left->val.integer <= right->val.integer);
  result->null = (left->null || right->null);
}

VM_OP_HOT void OpNotEqualInteger(tpl::sql::Integer *result,
                                 tpl::sql::Integer *left,
                                 tpl::sql::Integer *right) {
  result->val.boolean = (left->val.integer != right->val.integer);
  result->null = (left->null || right->null);
}

}  // extern "C"