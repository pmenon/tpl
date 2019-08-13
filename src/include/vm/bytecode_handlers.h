#pragma once

#include <cstdint>

#include "util/common.h"

#include "sql/aggregation_hash_table.h"
#include "sql/aggregators.h"
#include "sql/execution_context.h"
#include "sql/filter_manager.h"
#include "sql/functions/arithmetic_functions.h"
#include "sql/functions/comparison_functions.h"
#include "sql/functions/is_null_predicate.h"
#include "sql/functions/string_functions.h"
#include "sql/join_hash_table.h"
#include "sql/join_hash_table_vector_probe.h"
#include "sql/sorter.h"
#include "sql/table_vector_iterator.h"
#include "sql/thread_state_container.h"
#include "sql/vector_filter_executor.h"
#include "util/hash.h"
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

VM_OP_HOT void OpNot(bool *const result, const bool input) { *result = !input; }

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

VM_OP_HOT void OpIsNullPtr(bool *result, const void *const ptr) {
  *result = (ptr == nullptr);
}

VM_OP_HOT void OpIsNotNullPtr(bool *result, const void *const ptr) {
  *result = (ptr != nullptr);
}

VM_OP_HOT void OpDeref1(i8 *dest, const i8 *const src) { *dest = *src; }

VM_OP_HOT void OpDeref2(i16 *dest, const i16 *const src) { *dest = *src; }

VM_OP_HOT void OpDeref4(i32 *dest, const i32 *const src) { *dest = *src; }

VM_OP_HOT void OpDeref8(i64 *dest, const i64 *const src) { *dest = *src; }

VM_OP_HOT void OpDerefN(byte *dest, const byte *const src, u32 len) {
  std::memcpy(dest, src, len);
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

VM_OP_HOT bool OpJumpIfTrue(bool cond) { return cond; }

VM_OP_HOT bool OpJumpIfFalse(bool cond) { return !cond; }

VM_OP_HOT void OpCall(UNUSED u16 func_id, UNUSED u16 num_args) {}

VM_OP_HOT void OpReturn() {}

// ---------------------------------------------------------
// Execution Context
// ---------------------------------------------------------

VM_OP_HOT void OpExecutionContextGetMemoryPool(
    tpl::sql::MemoryPool **const memory,
    tpl::sql::ExecutionContext *const exec_ctx) {
  *memory = exec_ctx->memory_pool();
}

void OpThreadStateContainerInit(
    tpl::sql::ThreadStateContainer *thread_state_container,
    tpl::sql::MemoryPool *memory);

VM_OP_HOT void OpThreadStateContainerReset(
    tpl::sql::ThreadStateContainer *thread_state_container, u32 size,
    tpl::sql::ThreadStateContainer::InitFn init_fn,
    tpl::sql::ThreadStateContainer::DestroyFn destroy_fn, void *ctx) {
  thread_state_container->Reset(size, init_fn, destroy_fn, ctx);
}

VM_OP_HOT void OpThreadStateContainerIterate(
    tpl::sql::ThreadStateContainer *thread_state_container, void *const state,
    tpl::sql::ThreadStateContainer::IterateFn iterate_fn) {
  thread_state_container->IterateStates(state, iterate_fn);
}

void OpThreadStateContainerFree(
    tpl::sql::ThreadStateContainer *thread_state_container);

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorInit(tpl::sql::TableVectorIterator *iter,
                               u16 table_id);

void OpTableVectorIteratorPerformInit(tpl::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorNext(bool *has_more,
                                         tpl::sql::TableVectorIterator *iter) {
  *has_more = iter->Advance();
}

void OpTableVectorIteratorFree(tpl::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorGetVPI(
    tpl::sql::VectorProjectionIterator **vpi,
    tpl::sql::TableVectorIterator *iter) {
  *vpi = iter->vector_projection_iterator();
}

VM_OP_HOT void OpParallelScanTable(
    const u16 table_id, void *const query_state,
    tpl::sql::ThreadStateContainer *const thread_states,
    const tpl::sql::TableVectorIterator::ScanFn scanner) {
  tpl::sql::TableVectorIterator::ParallelScan(table_id, query_state,
                                              thread_states, scanner);
}

// ---------------------------------------------------------
// Vector Projection Iterator
// ---------------------------------------------------------

VM_OP_HOT void OpVPIIsFiltered(bool *is_filtered,
                               const tpl::sql::VectorProjectionIterator *vpi) {
  *is_filtered = vpi->IsFiltered();
}

VM_OP_HOT void OpVPIGetSelectedRowCount(
    u32 *count, const tpl::sql::VectorProjectionIterator *vpi) {
  *count = vpi->GetTupleCount();
}

VM_OP_HOT void OpVPIHasNext(bool *has_more,
                            const tpl::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNext();
}

VM_OP_HOT void OpVPIHasNextFiltered(
    bool *has_more, const tpl::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNextFiltered();
}

VM_OP_HOT void OpVPIAdvance(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->Advance();
}

VM_OP_HOT void OpVPIAdvanceFiltered(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->AdvanceFiltered();
}

VM_OP_HOT void OpVPISetPosition(tpl::sql::VectorProjectionIterator *vpi,
                                const u32 index) {
  vpi->SetPosition<false>(index);
}

VM_OP_HOT void OpVPISetPositionFiltered(
    tpl::sql::VectorProjectionIterator *const vpi, const u32 index) {
  vpi->SetPosition<true>(index);
}

VM_OP_HOT void OpVPIMatch(tpl::sql::VectorProjectionIterator *vpi,
                          const bool match) {
  vpi->Match(match);
}

VM_OP_HOT void OpVPIReset(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->Reset();
}

VM_OP_HOT void OpVPIResetFiltered(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->ResetFiltered();
}

VM_OP_HOT void OpVPIGetSmallInt(tpl::sql::Integer *out,
                                tpl::sql::VectorProjectionIterator *const iter,
                                const u32 col_idx) {
  // Read
  auto *ptr = iter->GetValue<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpVPIGetInteger(tpl::sql::Integer *out,
                               tpl::sql::VectorProjectionIterator *const vpi,
                               const u32 col_idx) {
  // Read
  auto *ptr = vpi->GetValue<i32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpVPIGetBigInt(tpl::sql::Integer *out,
                              tpl::sql::VectorProjectionIterator *const vpi,
                              const u32 col_idx) {
  // Read
  auto *ptr = vpi->GetValue<i64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpVPIGetReal(tpl::sql::Real *out,
                            tpl::sql::VectorProjectionIterator *const vpi,
                            const u32 col_idx) {
  // Read
  auto *ptr = vpi->GetValue<f32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read real value");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpVPIGetDouble(tpl::sql::Real *out,
                              tpl::sql::VectorProjectionIterator *const vpi,
                              const u32 col_idx) {
  // Read
  auto *ptr = vpi->GetValue<f64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read double value");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpVPIGetDecimal(tpl::sql::Decimal *out,
                               UNUSED tpl::sql::VectorProjectionIterator *vpi,
                               UNUSED const u32 col_idx) {
  // Set
  out->is_null = false;
  out->val = 0;
}

VM_OP_HOT void OpVPIGetSmallIntNull(
    tpl::sql::Integer *out, tpl::sql::VectorProjectionIterator *const vpi,
    const u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = vpi->GetValue<i16, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpVPIGetIntegerNull(
    tpl::sql::Integer *out, tpl::sql::VectorProjectionIterator *const vpi,
    const u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = vpi->GetValue<i32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpVPIGetBigIntNull(tpl::sql::Integer *out,
                                  tpl::sql::VectorProjectionIterator *const vpi,
                                  const u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = vpi->GetValue<i64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpVPIGetRealNull(tpl::sql::Real *out,
                                tpl::sql::VectorProjectionIterator *const vpi,
                                const u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = vpi->GetValue<f32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read real value");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpVPIGetDoubleNull(tpl::sql::Real *out,
                                  tpl::sql::VectorProjectionIterator *const vpi,
                                  const u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = vpi->GetValue<f64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read double value");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpVPIGetDecimalNull(
    tpl::sql::Decimal *out, tpl::sql::VectorProjectionIterator *const vpi,
    const u32 col_idx) {
  out->val = 0;
  out->is_null = false;
}

VM_OP_HOT void OpVPISetSmallInt(tpl::sql::VectorProjectionIterator *const vpi,
                                tpl::sql::Integer *input, const u32 col_idx) {
  vpi->SetValue<i16, false>(col_idx, input->val, false);
}

VM_OP_HOT void OpVPISetInteger(tpl::sql::VectorProjectionIterator *const vpi,
                               tpl::sql::Integer *input, const u32 col_idx) {
  vpi->SetValue<i32, false>(col_idx, input->val, false);
}

VM_OP_HOT void OpVPISetBigInt(tpl::sql::VectorProjectionIterator *const vpi,
                              tpl::sql::Integer *input, const u32 col_idx) {
  vpi->SetValue<i64, false>(col_idx, input->val, false);
}

VM_OP_HOT void OpVPISetReal(tpl::sql::VectorProjectionIterator *const vpi,
                            tpl::sql::Real *input, const u32 col_idx) {
  vpi->SetValue<f32, false>(col_idx, input->val, false);
}

VM_OP_HOT void OpVPISetDouble(tpl::sql::VectorProjectionIterator *const vpi,
                              tpl::sql::Real *input, const u32 col_idx) {
  vpi->SetValue<f64, false>(col_idx, input->val, false);
}

VM_OP_HOT void OpVPISetDecimal(tpl::sql::VectorProjectionIterator *const vpi,
                               tpl::sql::Decimal *input, const u32 col_idx) {
  // TODO(pmenon): Implement me
}

VM_OP_HOT void OpVPISetSmallIntNull(
    tpl::sql::VectorProjectionIterator *const vpi, tpl::sql::Integer *input,
    const u32 col_idx) {
  vpi->SetValue<i16, true>(col_idx, input->val, input->is_null);
}

VM_OP_HOT void OpVPISetIntegerNull(
    tpl::sql::VectorProjectionIterator *const vpi, tpl::sql::Integer *input,
    const u32 col_idx) {
  vpi->SetValue<i32, true>(col_idx, input->val, input->is_null);
}

VM_OP_HOT void OpVPISetBigIntNull(tpl::sql::VectorProjectionIterator *const vpi,
                                  tpl::sql::Integer *input, const u32 col_idx) {
  vpi->SetValue<i64, true>(col_idx, input->val, input->is_null);
}

VM_OP_HOT void OpVPISetRealNull(tpl::sql::VectorProjectionIterator *const vpi,
                                tpl::sql::Real *input, const u32 col_idx) {
  vpi->SetValue<f32, true>(col_idx, input->val, input->is_null);
}

VM_OP_HOT void OpVPISetDoubleNull(tpl::sql::VectorProjectionIterator *const vpi,
                                  tpl::sql::Real *input, const u32 col_idx) {
  vpi->SetValue<f64, true>(col_idx, input->val, input->is_null);
}

VM_OP_HOT void OpVPISetDecimalNull(
    tpl::sql::VectorProjectionIterator *const vpi, tpl::sql::Decimal *input,
    const u32 col_idx) {
  // TODO(pmenon): Implement me
}

// ---------------------------------------------------------
// Hashing
// ---------------------------------------------------------

VM_OP_HOT void OpHashInt(hash_t *const hash_val,
                         const tpl::sql::Integer *const input,
                         const hash_t seed) {
  *hash_val =
      tpl::util::Hasher::Hash<tpl::util::HashMethod::Crc>(input->val, seed);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashReal(hash_t *const hash_val,
                          const tpl::sql::Real *const input,
                          const hash_t seed) {
  *hash_val =
      tpl::util::Hasher::Hash<tpl::util::HashMethod::Crc>(input->val, seed);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashString(hash_t *const hash_val,
                            const tpl::sql::StringVal *const input,
                            const hash_t seed) {
  if (input->is_null) {
    *hash_val = 0;
  } else {
    *hash_val = tpl::util::Hasher::Hash<tpl::util::HashMethod::xxHash3>(
        reinterpret_cast<const u8 *>(input->ptr), input->len, seed);
  }
}

VM_OP_HOT void OpHashCombine(hash_t *hash_val, hash_t new_hash_val) {
  *hash_val = tpl::util::Hasher::CombineHashes(*hash_val, new_hash_val);
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

void OpFilterManagerInit(tpl::sql::FilterManager *filter_manager);

void OpFilterManagerStartNewClause(tpl::sql::FilterManager *filter_manager);

void OpFilterManagerInsertFlavor(tpl::sql::FilterManager *filter_manager,
                                 tpl::sql::FilterManager::MatchFn flavor);

void OpFilterManagerFinalize(tpl::sql::FilterManager *filter_manager);

void OpFilterManagerRunFilters(tpl::sql::FilterManager *filter,
                               tpl::sql::VectorProjectionIterator *vpi);

void OpFilterManagerFree(tpl::sql::FilterManager *filter);

// ---------------------------------------------------------
// Vector Filter Executor
// ---------------------------------------------------------

VM_OP_HOT void OpVectorFilterExecuteInit(
    tpl::sql::VectorFilterExecutor *filter_exec,
    tpl::sql::VectorProjectionIterator *vpi) {
  new (filter_exec) tpl::sql::VectorFilterExecutor(vpi);
}

VM_OP_HOT void OpVectorFilterExecuteEqual(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const u32 right_col_idx) {
  filter_exec->SelectEq(left_col_idx, right_col_idx);
}
VM_OP_HOT void OpVectorFilterExecuteEqualVal(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const tpl::sql::Val *val) {
  filter_exec->SelectEqVal(left_col_idx, *val);
}

VM_OP_HOT void OpVectorFilterExecuteGreaterThan(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const u32 right_col_idx) {
  filter_exec->SelectGt(left_col_idx, right_col_idx);
}

VM_OP_HOT void OpVectorFilterExecuteGreaterThanVal(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const tpl::sql::Val *val) {
  filter_exec->SelectGtVal(left_col_idx, *val);
}

VM_OP_HOT void OpVectorFilterExecuteGreaterThanEqual(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const u32 right_col_idx) {
  filter_exec->SelectGe(left_col_idx, right_col_idx);
}

VM_OP_HOT void OpVectorFilterExecuteGreaterThanEqualVal(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const tpl::sql::Val *val) {
  filter_exec->SelectGeVal(left_col_idx, *val);
}

VM_OP_HOT void OpVectorFilterExecuteLessThan(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const u32 right_col_idx) {
  filter_exec->SelectLt(left_col_idx, right_col_idx);
}

VM_OP_HOT void OpVectorFilterExecuteLessThanVal(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const tpl::sql::Val *val) {
  filter_exec->SelectLtVal(left_col_idx, *val);
}

VM_OP_HOT void OpVectorFilterExecuteLessThanEqual(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const u32 right_col_idx) {
  filter_exec->SelectLe(left_col_idx, right_col_idx);
}

VM_OP_HOT void OpVectorFilterExecuteLessThanEqualVal(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const tpl::sql::Val *val) {
  filter_exec->SelectLeVal(left_col_idx, *val);
}

VM_OP_HOT void OpVectorFilterExecuteNotEqual(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const u32 right_col_idx) {
  filter_exec->SelectNe(left_col_idx, right_col_idx);
}

VM_OP_HOT void OpVectorFilterExecuteNotEqualVal(
    tpl::sql::VectorFilterExecutor *filter_exec, const u32 left_col_idx,
    const tpl::sql::Val *val) {
  filter_exec->SelectNeVal(left_col_idx, *val);
}

VM_OP_HOT void OpVectorFilterExecuteFinish(
    tpl::sql::VectorFilterExecutor *filter_exec) {
  filter_exec->Finish();
}

VM_OP_HOT void OpVectorFilterExecuteFree(
    tpl::sql::VectorFilterExecutor *filter_exec) {
  filter_exec->~VectorFilterExecutor();
}

// ---------------------------------------------------------
// Scalar SQL comparisons
// ---------------------------------------------------------

VM_OP_HOT void OpForceBoolTruth(bool *result, tpl::sql::BoolVal *input) {
  *result = input->ForceTruth();
}

VM_OP_HOT void OpInitBool(tpl::sql::BoolVal *result, bool input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitInteger(tpl::sql::Integer *result, i32 input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitReal(tpl::sql::Real *result, double input) {
  result->is_null = false;
  result->val = input;
}

#define GEN_SQL_COMPARISONS(TYPE)                                              \
  VM_OP_HOT void OpGreaterThan##TYPE(tpl::sql::BoolVal *const result,          \
                                     const tpl::sql::TYPE *const left,         \
                                     const tpl::sql::TYPE *const right) {      \
    tpl::sql::ComparisonFunctions::Gt##TYPE(result, *left, *right);            \
  }                                                                            \
  VM_OP_HOT void OpGreaterThanEqual##TYPE(tpl::sql::BoolVal *const result,     \
                                          const tpl::sql::TYPE *const left,    \
                                          const tpl::sql::TYPE *const right) { \
    tpl::sql::ComparisonFunctions::Ge##TYPE(result, *left, *right);            \
  }                                                                            \
  VM_OP_HOT void OpEqual##TYPE(tpl::sql::BoolVal *const result,                \
                               const tpl::sql::TYPE *const left,               \
                               const tpl::sql::TYPE *const right) {            \
    tpl::sql::ComparisonFunctions::Eq##TYPE(result, *left, *right);            \
  }                                                                            \
  VM_OP_HOT void OpLessThan##TYPE(tpl::sql::BoolVal *const result,             \
                                  const tpl::sql::TYPE *const left,            \
                                  const tpl::sql::TYPE *const right) {         \
    tpl::sql::ComparisonFunctions::Lt##TYPE(result, *left, *right);            \
  }                                                                            \
  VM_OP_HOT void OpLessThanEqual##TYPE(tpl::sql::BoolVal *const result,        \
                                       const tpl::sql::TYPE *const left,       \
                                       const tpl::sql::TYPE *const right) {    \
    tpl::sql::ComparisonFunctions::Le##TYPE(result, *left, *right);            \
  }                                                                            \
  VM_OP_HOT void OpNotEqual##TYPE(tpl::sql::BoolVal *const result,             \
                                  const tpl::sql::TYPE *const left,            \
                                  const tpl::sql::TYPE *const right) {         \
    tpl::sql::ComparisonFunctions::Ne##TYPE(result, *left, *right);            \
  }

GEN_SQL_COMPARISONS(Integer)
GEN_SQL_COMPARISONS(Real)

#undef GEN_SQL_COMPARISONS

VM_OP_HOT void OpGreaterThanString(tpl::sql::BoolVal *const result,
                                   const tpl::sql::StringVal *const left,
                                   const tpl::sql::StringVal *const right) {
  tpl::sql::ComparisonFunctions::GtStringVal(result, *left, *right);
}

VM_OP_HOT void OpGreaterThanEqualString(
    tpl::sql::BoolVal *const result, const tpl::sql::StringVal *const left,
    const tpl::sql::StringVal *const right) {
  tpl::sql::ComparisonFunctions::GeStringVal(result, *left, *right);
}

VM_OP_HOT void OpEqualString(tpl::sql::BoolVal *const result,
                             const tpl::sql::StringVal *const left,
                             const tpl::sql::StringVal *const right) {
  tpl::sql::ComparisonFunctions::EqStringVal(result, *left, *right);
}

VM_OP_HOT void OpLessThanString(tpl::sql::BoolVal *const result,
                                const tpl::sql::StringVal *const left,
                                const tpl::sql::StringVal *const right) {
  tpl::sql::ComparisonFunctions::LtStringVal(result, *left, *right);
}

VM_OP_HOT void OpLessThanEqualString(tpl::sql::BoolVal *const result,
                                     const tpl::sql::StringVal *const left,
                                     const tpl::sql::StringVal *const right) {
  tpl::sql::ComparisonFunctions::LeStringVal(result, *left, *right);
}

VM_OP_HOT void OpNotEqualString(tpl::sql::BoolVal *const result,
                                const tpl::sql::StringVal *const left,
                                const tpl::sql::StringVal *const right) {
  tpl::sql::ComparisonFunctions::NeStringVal(result, *left, *right);
}

VM_OP_WARM void OpAbsInteger(tpl::sql::Integer *const result,
                             const tpl::sql::Integer *const left) {
  tpl::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_WARM void OpAbsReal(tpl::sql::Real *const result,
                          const tpl::sql::Real *const left) {
  tpl::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_HOT void OpAddInteger(tpl::sql::Integer *const result,
                            const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool overflow;
  tpl::sql::ArithmeticFunctions::Add(result, *left, *right, &overflow);
}

VM_OP_HOT void OpSubInteger(tpl::sql::Integer *const result,
                            const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool overflow;
  tpl::sql::ArithmeticFunctions::Sub(result, *left, *right, &overflow);
}

VM_OP_HOT void OpMulInteger(tpl::sql::Integer *const result,
                            const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool overflow;
  tpl::sql::ArithmeticFunctions::Mul(result, *left, *right, &overflow);
}

VM_OP_HOT void OpDivInteger(tpl::sql::Integer *const result,
                            const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::IntDiv(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpRemInteger(tpl::sql::Integer *const result,
                            const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::IntMod(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpAddReal(tpl::sql::Real *const result,
                         const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  tpl::sql::ArithmeticFunctions::Add(result, *left, *right);
}

VM_OP_HOT void OpSubReal(tpl::sql::Real *const result,
                         const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  tpl::sql::ArithmeticFunctions::Sub(result, *left, *right);
}

VM_OP_HOT void OpMulReal(tpl::sql::Real *const result,
                         const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  tpl::sql::ArithmeticFunctions::Mul(result, *left, *right);
}

VM_OP_HOT void OpDivReal(tpl::sql::Real *const result,
                         const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::Div(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpRemReal(tpl::sql::Real *const result,
                         const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::Mod(result, *left, *right, &div_by_zero);
}

// ---------------------------------------------------------
// SQL Aggregations
// ---------------------------------------------------------

void OpAggregationHashTableInit(tpl::sql::AggregationHashTable *agg_hash_table,
                                tpl::sql::MemoryPool *memory, u32 payload_size);

VM_OP_HOT void OpAggregationHashTableInsert(
    byte **result, tpl::sql::AggregationHashTable *agg_hash_table,
    hash_t hash_val) {
  *result = agg_hash_table->Insert(hash_val);
}

VM_OP_HOT void OpAggregationHashTableInsertPartitioned(
    byte **result, tpl::sql::AggregationHashTable *agg_hash_table,
    hash_t hash_val) {
  *result = agg_hash_table->InsertPartitioned(hash_val);
}

VM_OP_HOT void OpAggregationHashTableLookup(
    byte **result, tpl::sql::AggregationHashTable *const agg_hash_table,
    const hash_t hash_val,
    const tpl::sql::AggregationHashTable::KeyEqFn key_eq_fn,
    const void *probe_tuple) {
  *result = agg_hash_table->Lookup(hash_val, key_eq_fn, probe_tuple);
}

VM_OP_HOT void OpAggregationHashTableProcessBatch(
    tpl::sql::AggregationHashTable *const agg_hash_table,
    tpl::sql::VectorProjectionIterator *iters[],
    const tpl::sql::AggregationHashTable::HashFn hash_fn,
    const tpl::sql::AggregationHashTable::KeyEqFn key_eq_fn,
    const tpl::sql::AggregationHashTable::InitAggFn init_agg_fn,
    const tpl::sql::AggregationHashTable::AdvanceAggFn merge_agg_fn,
    const bool partitioned) {
  agg_hash_table->ProcessBatch(iters, hash_fn, key_eq_fn, init_agg_fn,
                               merge_agg_fn, partitioned);
}

VM_OP_HOT void OpAggregationHashTableTransferPartitions(
    tpl::sql::AggregationHashTable *const agg_hash_table,
    tpl::sql::ThreadStateContainer *const thread_state_container,
    const u32 agg_ht_offset,
    const tpl::sql::AggregationHashTable::MergePartitionFn merge_partition_fn) {
  agg_hash_table->TransferMemoryAndPartitions(
      thread_state_container, agg_ht_offset, merge_partition_fn);
}

VM_OP_HOT void OpAggregationHashTableParallelPartitionedScan(
    tpl::sql::AggregationHashTable *const agg_hash_table,
    void *const query_state,
    tpl::sql::ThreadStateContainer *const thread_state_container,
    const tpl::sql::AggregationHashTable::ScanPartitionFn scan_partition_fn) {
  agg_hash_table->ExecuteParallelPartitionedScan(
      query_state, thread_state_container, scan_partition_fn);
}

void OpAggregationHashTableFree(tpl::sql::AggregationHashTable *agg_hash_table);

void OpAggregationHashTableIteratorInit(
    tpl::sql::AHTIterator *iter,
    tpl::sql::AggregationHashTable *agg_hash_table);

VM_OP_HOT void OpAggregationHashTableIteratorHasNext(
    bool *has_more, tpl::sql::AHTIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationHashTableIteratorNext(tpl::sql::AHTIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpAggregationHashTableIteratorGetRow(
    const byte **row, tpl::sql::AHTIterator *iter) {
  *row = iter->GetCurrentAggregateRow();
}

void OpAggregationHashTableIteratorFree(tpl::sql::AHTIterator *iter);

VM_OP_HOT void OpAggregationOverflowPartitionIteratorHasNext(
    bool *has_more, tpl::sql::AHTOverflowPartitionIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorNext(
    tpl::sql::AHTOverflowPartitionIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetHash(
    hash_t *hash_val, tpl::sql::AHTOverflowPartitionIterator *iter) {
  *hash_val = iter->GetHash();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRow(
    const byte **row, tpl::sql::AHTOverflowPartitionIterator *iter) {
  *row = iter->GetPayload();
}

//
// COUNT
//

VM_OP_HOT void OpCountAggregateInit(tpl::sql::CountAggregate *agg) {
  new (agg) tpl::sql::CountAggregate();
}

VM_OP_HOT void OpCountAggregateAdvance(tpl::sql::CountAggregate *agg,
                                       const tpl::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountAggregateMerge(tpl::sql::CountAggregate *agg_1,
                                     const tpl::sql::CountAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountAggregateReset(tpl::sql::CountAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpCountAggregateGetResult(tpl::sql::Integer *result,
                                         const tpl::sql::CountAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountAggregateFree(tpl::sql::CountAggregate *agg) {
  agg->~CountAggregate();
}

//
// COUNT(*)
//

VM_OP_HOT void OpCountStarAggregateInit(tpl::sql::CountStarAggregate *agg) {
  new (agg) tpl::sql::CountStarAggregate();
}

VM_OP_HOT void OpCountStarAggregateAdvance(tpl::sql::CountStarAggregate *agg,
                                           const tpl::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountStarAggregateMerge(
    tpl::sql::CountStarAggregate *agg_1,
    const tpl::sql::CountStarAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountStarAggregateReset(tpl::sql::CountStarAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpCountStarAggregateGetResult(
    tpl::sql::Integer *result, const tpl::sql::CountStarAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountStarAggregateFree(tpl::sql::CountStarAggregate *agg) {
  agg->~CountStarAggregate();
}

//
// SUM(int_type)
//

VM_OP_HOT void OpIntegerSumAggregateInit(tpl::sql::IntegerSumAggregate *agg) {
  new (agg) tpl::sql::IntegerSumAggregate();
}

VM_OP_HOT void OpIntegerSumAggregateAdvance(tpl::sql::IntegerSumAggregate *agg,
                                            const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerSumAggregateMerge(
    tpl::sql::IntegerSumAggregate *agg_1,
    const tpl::sql::IntegerSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerSumAggregateReset(tpl::sql::IntegerSumAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpIntegerSumAggregateGetResult(
    tpl::sql::Integer *result, const tpl::sql::IntegerSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpIntegerSumAggregateFree(tpl::sql::IntegerSumAggregate *agg) {
  agg->~IntegerSumAggregate();
}

//
// MAX(int_type)
//

VM_OP_HOT void OpIntegerMaxAggregateInit(tpl::sql::IntegerMaxAggregate *agg) {
  new (agg) tpl::sql::IntegerMaxAggregate();
}

VM_OP_HOT void OpIntegerMaxAggregateAdvance(tpl::sql::IntegerMaxAggregate *agg,
                                            const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMaxAggregateMerge(
    tpl::sql::IntegerMaxAggregate *agg_1,
    const tpl::sql::IntegerMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMaxAggregateReset(tpl::sql::IntegerMaxAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpIntegerMaxAggregateGetResult(
    tpl::sql::Integer *result, const tpl::sql::IntegerMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpIntegerMaxAggregateFree(tpl::sql::IntegerMaxAggregate *agg) {
  agg->~IntegerMaxAggregate();
}

//
// MIN(int_type)
//

VM_OP_HOT void OpIntegerMinAggregateInit(tpl::sql::IntegerMinAggregate *agg) {
  new (agg) tpl::sql::IntegerMinAggregate();
}

VM_OP_HOT void OpIntegerMinAggregateAdvance(tpl::sql::IntegerMinAggregate *agg,
                                            const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMinAggregateMerge(
    tpl::sql::IntegerMinAggregate *agg_1,
    const tpl::sql::IntegerMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMinAggregateReset(tpl::sql::IntegerMinAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpIntegerMinAggregateGetResult(
    tpl::sql::Integer *result, const tpl::sql::IntegerMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpIntegerMinAggregateFree(tpl::sql::IntegerMinAggregate *agg) {
  agg->~IntegerMinAggregate();
}

//
// SUM(real)
//

VM_OP_HOT void OpRealSumAggregateInit(tpl::sql::RealSumAggregate *agg) {
  new (agg) tpl::sql::RealSumAggregate();
}

VM_OP_HOT void OpRealSumAggregateAdvance(tpl::sql::RealSumAggregate *agg,
                                         const tpl::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealSumAggregateMerge(
    tpl::sql::RealSumAggregate *agg_1,
    const tpl::sql::RealSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealSumAggregateReset(tpl::sql::RealSumAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpRealSumAggregateGetResult(
    tpl::sql::Real *result, const tpl::sql::RealSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpRealSumAggregateFree(tpl::sql::RealSumAggregate *agg) {
  agg->~RealSumAggregate();
}

//
// MAX(real_type)
//

VM_OP_HOT void OpRealMaxAggregateInit(tpl::sql::RealMaxAggregate *agg) {
  new (agg) tpl::sql::RealMaxAggregate();
}

VM_OP_HOT void OpRealMaxAggregateAdvance(tpl::sql::RealMaxAggregate *agg,
                                         const tpl::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMaxAggregateMerge(
    tpl::sql::RealMaxAggregate *agg_1,
    const tpl::sql::RealMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMaxAggregateReset(tpl::sql::RealMaxAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpRealMaxAggregateGetResult(
    tpl::sql::Real *result, const tpl::sql::RealMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpRealMaxAggregateFree(tpl::sql::RealMaxAggregate *agg) {
  agg->~RealMaxAggregate();
}

//
// MIN(real_type)
//

VM_OP_HOT void OpRealMinAggregateInit(tpl::sql::RealMinAggregate *agg) {
  new (agg) tpl::sql::RealMinAggregate();
}

VM_OP_HOT void OpRealMinAggregateAdvance(tpl::sql::RealMinAggregate *agg,
                                         const tpl::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMinAggregateMerge(
    tpl::sql::RealMinAggregate *agg_1,
    const tpl::sql::RealMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMinAggregateReset(tpl::sql::RealMinAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpRealMinAggregateGetResult(
    tpl::sql::Real *result, const tpl::sql::RealMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpRealMinAggregateFree(tpl::sql::RealMinAggregate *agg) {
  agg->~RealMinAggregate();
}

//
// AVG
//

VM_OP_HOT void OpAvgAggregateInit(tpl::sql::AvgAggregate *agg) {
  new (agg) tpl::sql::AvgAggregate();
}

VM_OP_HOT void OpAvgAggregateAdvance(tpl::sql::AvgAggregate *agg,
                                     const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpAvgAggregateMerge(tpl::sql::AvgAggregate *agg_1,
                                   const tpl::sql::AvgAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpAvgAggregateReset(tpl::sql::AvgAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpAvgAggregateGetResult(tpl::sql::Real *result,
                                       const tpl::sql::AvgAggregate *agg) {
  *result = agg->GetResultAvg();
}

VM_OP_HOT void OpAvgAggregateFree(tpl::sql::AvgAggregate *agg) {
  agg->~AvgAggregate();
}

// ---------------------------------------------------------
// Hash Joins
// ---------------------------------------------------------

void OpJoinHashTableInit(tpl::sql::JoinHashTable *join_hash_table,
                         tpl::sql::MemoryPool *memory, u32 tuple_size);

VM_OP_HOT void OpJoinHashTableAllocTuple(
    byte **result, tpl::sql::JoinHashTable *join_hash_table, hash_t hash) {
  *result = join_hash_table->AllocInputTuple(hash);
}

void OpJoinHashTableBuild(tpl::sql::JoinHashTable *join_hash_table);

void OpJoinHashTableBuildParallel(
    tpl::sql::JoinHashTable *join_hash_table,
    tpl::sql::ThreadStateContainer *thread_state_container, u32 jht_offset);

VM_OP_HOT void OpJoinHashTableLookup(
    tpl::sql::JoinHashTable *join_hash_table,
    tpl::sql::HashTableEntryIterator *ht_entry_iter, const hash_t hash_val) {
  *ht_entry_iter = join_hash_table->Lookup<false>(hash_val);
}

void OpJoinHashTableFree(tpl::sql::JoinHashTable *join_hash_table);

void OpJoinHashTableVectorProbeInit(
    tpl::sql::JoinHashTableVectorProbe *jht_vector_probe,
    tpl::sql::JoinHashTable *jht);

VM_OP_HOT void OpJoinHashTableVectorProbePrepare(
    tpl::sql::JoinHashTableVectorProbe *jht_vector_probe,
    tpl::sql::VectorProjectionIterator *vpi,
    tpl::sql::JoinHashTableVectorProbe::HashFn hash_fn) {
  jht_vector_probe->Prepare(vpi, hash_fn);
}

VM_OP_HOT void OpJoinHashTableVectorProbeGetNextOutput(
    const byte **result, tpl::sql::JoinHashTableVectorProbe *jht_vector_probe,
    tpl::sql::VectorProjectionIterator *vpi,
    tpl::sql::JoinHashTableVectorProbe::KeyEqFn key_eq_fn) {
  *result = jht_vector_probe->GetNextOutput(vpi, key_eq_fn);
}

void OpJoinHashTableVectorProbeFree(
    tpl::sql::JoinHashTableVectorProbe *jht_vector_probe);

VM_OP_HOT void OpHashTableEntryIteratorHasNext(
    bool *has_next, tpl::sql::HashTableEntryIterator *ht_entry_iter,
    tpl::sql::HashTableEntryIterator::KeyEq key_eq, void *ctx,
    void *probe_tuple) {
  *has_next = ht_entry_iter->HasNext(key_eq, ctx, probe_tuple);
}

VM_OP_HOT void OpHashTableEntryIteratorGetRow(
    const byte **row, tpl::sql::HashTableEntryIterator *ht_entry_iter) {
  *row = ht_entry_iter->NextMatch()->PayloadAs<byte>();
}

// ---------------------------------------------------------
// Sorting
// ---------------------------------------------------------

void OpSorterInit(tpl::sql::Sorter *sorter, tpl::sql::MemoryPool *memory,
                  tpl::sql::Sorter::ComparisonFunction cmp_fn, u32 tuple_size);

VM_OP_HOT void OpSorterAllocTuple(byte **result, tpl::sql::Sorter *sorter) {
  *result = sorter->AllocInputTuple();
}

VM_OP_HOT void OpSorterAllocTupleTopK(byte **result, tpl::sql::Sorter *sorter,
                                      u64 top_k) {
  *result = sorter->AllocInputTupleTopK(top_k);
}

VM_OP_HOT void OpSorterAllocTupleTopKFinish(tpl::sql::Sorter *sorter,
                                            u64 top_k) {
  sorter->AllocInputTupleTopKFinish(top_k);
}

void OpSorterSort(tpl::sql::Sorter *sorter);

void OpSorterSortParallel(
    tpl::sql::Sorter *sorter,
    tpl::sql::ThreadStateContainer *thread_state_container, u32 sorter_offset);

void OpSorterSortTopKParallel(
    tpl::sql::Sorter *sorter,
    tpl::sql::ThreadStateContainer *thread_state_container, u32 sorter_offset,
    u64 top_k);

void OpSorterFree(tpl::sql::Sorter *sorter);

void OpSorterIteratorInit(tpl::sql::SorterIterator *iter,
                          tpl::sql::Sorter *sorter);

VM_OP_HOT void OpSorterIteratorHasNext(bool *has_more,
                                       tpl::sql::SorterIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpSorterIteratorNext(tpl::sql::SorterIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpSorterIteratorGetRow(const byte **row,
                                      tpl::sql::SorterIterator *iter) {
  *row = iter->GetRow();
}

void OpSorterIteratorFree(tpl::sql::SorterIterator *iter);

// ---------------------------------------------------------
// Trig functions
// ---------------------------------------------------------

VM_OP_WARM void OpPi(tpl::sql::Real *result) {
  tpl::sql::ArithmeticFunctions::Pi(result);
}

VM_OP_WARM void OpE(tpl::sql::Real *result) {
  tpl::sql::ArithmeticFunctions::E(result);
}

VM_OP_WARM void OpAcos(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Acos(result, *input);
}

VM_OP_WARM void OpAsin(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Asin(result, *input);
}

VM_OP_WARM void OpAtan(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Atan(result, *input);
}

VM_OP_WARM void OpAtan2(tpl::sql::Real *result, const tpl::sql::Real *arg_1,
                        const tpl::sql::Real *arg_2) {
  tpl::sql::ArithmeticFunctions::Atan2(result, *arg_1, *arg_2);
}

VM_OP_WARM void OpCos(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Cos(result, *input);
}

VM_OP_WARM void OpCot(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Cot(result, *input);
}

VM_OP_WARM void OpSin(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Sin(result, *input);
}

VM_OP_WARM void OpTan(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Tan(result, *input);
}

VM_OP_WARM void OpCosh(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Cosh(result, *v);
}

VM_OP_WARM void OpTanh(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Tanh(result, *v);
}

VM_OP_WARM void OpSinh(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Sinh(result, *v);
}

VM_OP_WARM void OpSqrt(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Sqrt(result, *v);
}

VM_OP_WARM void OpCbrt(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Cbrt(result, *v);
}

VM_OP_WARM void OpExp(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Exp(result, *v);
}

VM_OP_WARM void OpCeil(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Ceil(result, *v);
}

VM_OP_WARM void OpFloor(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Floor(result, *v);
}

VM_OP_WARM void OpTruncate(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Truncate(result, *v);
}

VM_OP_WARM void OpLn(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Ln(result, *v);
}

VM_OP_WARM void OpLog2(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Log2(result, *v);
}

VM_OP_WARM void OpLog10(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Log10(result, *v);
}

VM_OP_WARM void OpSign(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Sign(result, *v);
}

VM_OP_WARM void OpRadians(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Radians(result, *v);
}

VM_OP_WARM void OpDegrees(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Degrees(result, *v);
}

VM_OP_WARM void OpRound(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Round(result, *v);
}

VM_OP_WARM void OpRoundUpTo(tpl::sql::Real *result, const tpl::sql::Real *v,
                            const tpl::sql::Integer *scale) {
  tpl::sql::ArithmeticFunctions::RoundUpTo(result, *v, *scale);
}

VM_OP_WARM void OpLog(tpl::sql::Real *result, const tpl::sql::Real *base,
                      const tpl::sql::Real *val) {
  tpl::sql::ArithmeticFunctions::Log(result, *base, *val);
}

VM_OP_WARM void OpPow(tpl::sql::Real *result, const tpl::sql::Real *base,
                      const tpl::sql::Real *val) {
  tpl::sql::ArithmeticFunctions::Pow(result, *base, *val);
}

// ---------------------------------------------------------
// Null/Not Null predicates
// ---------------------------------------------------------

VM_OP_WARM void OpValIsNull(tpl::sql::BoolVal *result,
                            const tpl::sql::Val *val) {
  tpl::sql::IsNullPredicate::IsNull(result, *val);
}

VM_OP_WARM void OpValIsNotNull(tpl::sql::BoolVal *result,
                               const tpl::sql::Val *val) {
  tpl::sql::IsNullPredicate::IsNotNull(result, *val);
}

// ---------------------------------------------------------
// String functions
// ---------------------------------------------------------

VM_OP_WARM void OpCharLength(tpl::sql::ExecutionContext *ctx,
                             tpl::sql::Integer *result,
                             const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::CharLength(ctx, result, *str);
}

VM_OP_WARM void OpLeft(tpl::sql::ExecutionContext *ctx,
                       tpl::sql::StringVal *result,
                       const tpl::sql::StringVal *str,
                       const tpl::sql::Integer *n) {
  tpl::sql::StringFunctions::Left(ctx, result, *str, *n);
}

VM_OP_WARM void OpLength(tpl::sql::ExecutionContext *ctx,
                         tpl::sql::Integer *result,
                         const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Length(ctx, result, *str);
}

VM_OP_WARM void OpLower(tpl::sql::ExecutionContext *ctx,
                        tpl::sql::StringVal *result,
                        const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Lower(ctx, result, *str);
}

VM_OP_WARM void OpLPad(tpl::sql::ExecutionContext *ctx,
                       tpl::sql::StringVal *result,
                       const tpl::sql::StringVal *str,
                       const tpl::sql::Integer *len,
                       const tpl::sql::StringVal *pad) {
  tpl::sql::StringFunctions::Lpad(ctx, result, *str, *len, *pad);
}

VM_OP_WARM void OpLTrim(tpl::sql::ExecutionContext *ctx,
                        tpl::sql::StringVal *result,
                        const tpl::sql::StringVal *str,
                        const tpl::sql::StringVal *chars) {
  tpl::sql::StringFunctions::Ltrim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpRepeat(tpl::sql::ExecutionContext *ctx,
                         tpl::sql::StringVal *result,
                         const tpl::sql::StringVal *str,
                         const tpl::sql::Integer *n) {
  tpl::sql::StringFunctions::Repeat(ctx, result, *str, *n);
}

VM_OP_WARM void OpReverse(tpl::sql::ExecutionContext *ctx,
                          tpl::sql::StringVal *result,
                          const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Reverse(ctx, result, *str);
}

VM_OP_WARM void OpRight(tpl::sql::ExecutionContext *ctx,
                        tpl::sql::StringVal *result,
                        const tpl::sql::StringVal *str,
                        const tpl::sql::Integer *n) {
  tpl::sql::StringFunctions::Right(ctx, result, *str, *n);
}

VM_OP_WARM void OpRPad(tpl::sql::ExecutionContext *ctx,
                       tpl::sql::StringVal *result,
                       const tpl::sql::StringVal *str,
                       const tpl::sql::Integer *n,
                       const tpl::sql::StringVal *pad) {
  tpl::sql::StringFunctions::Rpad(ctx, result, *str, *n, *pad);
}

VM_OP_WARM void OpRTrim(tpl::sql::ExecutionContext *ctx,
                        tpl::sql::StringVal *result,
                        const tpl::sql::StringVal *str,
                        const tpl::sql::StringVal *chars) {
  tpl::sql::StringFunctions::Rtrim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpSplitPart(tpl::sql::ExecutionContext *ctx,
                            tpl::sql::StringVal *result,
                            const tpl::sql::StringVal *str,
                            const tpl::sql::StringVal *delim,
                            const tpl::sql::Integer *field) {
  tpl::sql::StringFunctions::SplitPart(ctx, result, *str, *delim, *field);
}

VM_OP_WARM void OpSubstring(tpl::sql::ExecutionContext *ctx,
                            tpl::sql::StringVal *result,
                            const tpl::sql::StringVal *str,
                            const tpl::sql::Integer *pos,
                            const tpl::sql::Integer *len) {
  tpl::sql::StringFunctions::Substring(ctx, result, *str, *pos, *len);
}

VM_OP_WARM void OpTrim(tpl::sql::ExecutionContext *ctx,
                       tpl::sql::StringVal *result,
                       const tpl::sql::StringVal *str,
                       const tpl::sql::StringVal *chars) {
  tpl::sql::StringFunctions::Trim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpUpper(tpl::sql::ExecutionContext *ctx,
                        tpl::sql::StringVal *result,
                        const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Upper(ctx, result, *str);
}

}  // extern "C"
