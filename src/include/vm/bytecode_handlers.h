#pragma once

#include <cstdint>

#include "util/common.h"

#include "sql/table_vector_iterator.h"
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

#define COMPARISONS(type, ...)                                                \
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

#define ARITHMETIC(type, ...)                                       \
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

#define BITS(type, ...)                                                \
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
// Table Vector Iterator
// ---------------------------------------------------------

VM_OP_COLD void OpTableVectorIteratorInit(tpl::sql::TableVectorIterator *iter,
                                          u16 table_id);

VM_OP_HOT void OpTableVectorIteratorNext(bool *has_more,
                                         tpl::sql::TableVectorIterator *iter) {
  *has_more = iter->Advance();
}

VM_OP_COLD void OpTableVectorIteratorClose(tpl::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorGetVPI(
    tpl::sql::VectorProjectionIterator **vpi,
    tpl::sql::TableVectorIterator *iter) {
  *vpi = iter->vector_projection_iterator();
}

VM_OP_HOT void OpVPIHasNext(bool *has_more,
                            tpl::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNext();
}

VM_OP_HOT void OpVPIAdvance(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->Advance();
}

VM_OP_HOT void OpVPIReset(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->Reset();
}

VM_OP_HOT void OpVPIGetSmallInt(tpl::sql::Integer *out,
                                tpl::sql::VectorProjectionIterator *iter,
                                u32 col_idx) {
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");
  out->val.smallint = *ptr;
}

VM_OP_HOT void OpVPIGetInteger(tpl::sql::Integer *out,
                               tpl::sql::VectorProjectionIterator *iter,
                               u32 col_idx) {
  auto *ptr = iter->Get<i32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");
  out->val.integer = *ptr;
}

VM_OP_HOT void OpVPIGetBigInt(tpl::sql::Integer *out,
                              tpl::sql::VectorProjectionIterator *iter,
                              u32 col_idx) {
  auto *ptr = iter->Get<i64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");
  out->val.bigint = *ptr;
}

VM_OP_HOT void OpVPIGetDecimal(tpl::sql::Decimal *out,
                               tpl::sql::VectorProjectionIterator *iter,
                               u32 col_idx) {
  out->val = 0;
  out->null = false;
}

VM_OP_HOT void OpVPIGetSmallIntNull(tpl::sql::Integer *out,
                                    tpl::sql::VectorProjectionIterator *iter,
                                    u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i16, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->null = null;
  out->val.smallint = *ptr;
}

VM_OP_HOT void OpVPIGetIntegerNull(tpl::sql::Integer *out,
                                   tpl::sql::VectorProjectionIterator *iter,
                                   u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->null = null;
  out->val.integer = *ptr;
}

VM_OP_HOT void OpVPIGetBigIntNull(tpl::sql::Integer *out,
                                  tpl::sql::VectorProjectionIterator *iter,
                                  u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->null = null;
  out->val.bigint = *ptr;
}

VM_OP_HOT void OpVPIGetDecimalNull(tpl::sql::Decimal *out,
                                   tpl::sql::VectorProjectionIterator *iter,
                                   u32 col_idx) {
  out->val = 0;
  out->null = false;
}

VM_OP_COLD void OpVPIFilterEqual(u32 *size,
                                 tpl::sql::VectorProjectionIterator *iter,
                                 u16 col_id, i64 val);

VM_OP_COLD void OpVPIFilterGreaterThan(u32 *size,
                                       tpl::sql::VectorProjectionIterator *iter,
                                       u16 col_id, i64 val);

VM_OP_COLD void OpVPIFilterGreaterThanEqual(
    u32 *size, tpl::sql::VectorProjectionIterator *iter, u16 col_id, i64 val);

VM_OP_COLD void OpVPIFilterLessThan(u32 *size,
                                    tpl::sql::VectorProjectionIterator *iter,
                                    u16 col_id, i64 val);

VM_OP_COLD void OpVPIFilterLessThanEqual(
    u32 *size, tpl::sql::VectorProjectionIterator *iter, u16 col_id, i64 val);

VM_OP_COLD void OpVPIFilterNotEqual(u32 *size,
                                    tpl::sql::VectorProjectionIterator *iter,
                                    u16 col_id, i64 val);

// ---------------------------------------------------------
// Scalar SQL comparisons
// ---------------------------------------------------------

VM_OP_HOT void OpForceBoolTruth(bool *result, tpl::sql::BoolVal *input) {
  *result = input->ForceTruth();
}

VM_OP_HOT void OpInitInteger(tpl::sql::Integer *result, i32 input) {
  result->val.integer = input;
  result->null = false;
}

VM_OP_HOT void OpGreaterThanInteger(tpl::sql::BoolVal *result,
                                    tpl::sql::Integer *left,
                                    tpl::sql::Integer *right) {
  result->val = (left->val.integer > right->val.integer);
  result->null = (left->null || right->null);
}

VM_OP_HOT void OpGreaterThanEqualInteger(tpl::sql::BoolVal *result,
                                         tpl::sql::Integer *left,
                                         tpl::sql::Integer *right) {
  result->val = (left->val.integer >= right->val.integer);
  result->null = (left->null || right->null);
}

VM_OP_HOT void OpEqualInteger(tpl::sql::BoolVal *result,
                              tpl::sql::Integer *left,
                              tpl::sql::Integer *right) {
  result->val = (left->val.integer == right->val.integer);
  result->null = (left->null || right->null);
}

VM_OP_HOT void OpLessThanInteger(tpl::sql::BoolVal *result,
                                 tpl::sql::Integer *left,
                                 tpl::sql::Integer *right) {
  result->val = (left->val.integer < right->val.integer);
  result->null = (left->null || right->null);
}

VM_OP_HOT void OpLessThanEqualInteger(tpl::sql::BoolVal *result,
                                      tpl::sql::Integer *left,
                                      tpl::sql::Integer *right) {
  result->val = (left->val.integer <= right->val.integer);
  result->null = (left->null || right->null);
}

VM_OP_HOT void OpNotEqualInteger(tpl::sql::BoolVal *result,
                                 tpl::sql::Integer *left,
                                 tpl::sql::Integer *right) {
  result->val = (left->val.integer != right->val.integer);
  result->null = (left->null || right->null);
}

}  // extern "C"
