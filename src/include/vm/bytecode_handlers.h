#pragma once

#include <cstdint>

#include "util/common.h"

#include "sql/aggregation_hash_table.h"
#include "sql/aggregators.h"
#include "sql/execution_context.h"
#include "sql/filter_manager.h"
#include "sql/functions/arithmetic_functions.h"
#include "sql/functions/comparison_functions.h"
#include "sql/join_hash_table.h"
#include "sql/join_hash_table_vector_probe.h"
#include "sql/sorter.h"
#include "sql/table_vector_iterator.h"
#include "sql/thread_state_container.h"
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

VM_OP_HOT void OpVPIIsFiltered(bool *is_filtered,
                               tpl::sql::VectorProjectionIterator *vpi) {
  *is_filtered = vpi->IsFiltered();
}

VM_OP_HOT void OpVPIHasNext(bool *has_more,
                            tpl::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNext();
}

VM_OP_HOT void OpVPIHasNextFiltered(bool *has_more,
                                    tpl::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNextFiltered();
}

VM_OP_HOT void OpVPIAdvance(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->Advance();
}

VM_OP_HOT void OpVPIAdvanceFiltered(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->AdvanceFiltered();
}

VM_OP_HOT void OpVPIMatch(tpl::sql::VectorProjectionIterator *vpi, bool match) {
  vpi->Match(match);
}

VM_OP_HOT void OpVPIReset(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->Reset();
}

VM_OP_HOT void OpVPIResetFiltered(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->ResetFiltered();
}

VM_OP_HOT void OpVPIGetSmallInt(tpl::sql::Integer *out,
                                tpl::sql::VectorProjectionIterator *iter,
                                u32 col_idx) {
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

void OpVPIFilterEqual(u32 *size, tpl::sql::VectorProjectionIterator *vpi,
                      u16 col_id, i64 val);

void OpVPIFilterGreaterThan(u32 *size, tpl::sql::VectorProjectionIterator *vpi,
                            u16 col_id, i64 val);

void OpVPIFilterGreaterThanEqual(u32 *size,
                                 tpl::sql::VectorProjectionIterator *vpi,
                                 u16 col_id, i64 val);

void OpVPIFilterLessThan(u32 *size, tpl::sql::VectorProjectionIterator *vpi,
                         u16 col_id, i64 val);

void OpVPIFilterLessThanEqual(u32 *size,
                              tpl::sql::VectorProjectionIterator *vpi,
                              u16 col_id, i64 val);

void OpVPIFilterNotEqual(u32 *size, tpl::sql::VectorProjectionIterator *vpi,
                         u16 col_id, i64 val);

// ---------------------------------------------------------
// Hashing
// ---------------------------------------------------------

VM_OP_HOT void OpHashInt(hash_t *hash_val, tpl::sql::Integer *input) {
  *hash_val = tpl::util::Hasher::Hash(reinterpret_cast<u8 *>(&input->val),
                                      sizeof(input->val));
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashReal(hash_t *hash_val, tpl::sql::Real *input) {
  *hash_val = tpl::util::Hasher::Hash(reinterpret_cast<u8 *>(&input->val),
                                      sizeof(input->val));
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashString(hash_t *hash_val, tpl::sql::StringVal *input) {
  *hash_val =
      tpl::util::Hasher::Hash(reinterpret_cast<u8 *>(input->ptr), input->len);
  *hash_val = input->is_null ? 0 : *hash_val;
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

VM_OP_HOT void OpGreaterThanInteger(tpl::sql::BoolVal *const result,
                                    const tpl::sql::Integer *const left,
                                    const tpl::sql::Integer *const right) {
  tpl::sql::ComparisonFunctions::GtInteger(result, *left, *right);
}

VM_OP_HOT void OpGreaterThanEqualInteger(tpl::sql::BoolVal *const result,
                                         tpl::sql::Integer *left,
                                         tpl::sql::Integer *right) {
  tpl::sql::ComparisonFunctions::GeInteger(result, *left, *right);
}

VM_OP_HOT void OpEqualInteger(tpl::sql::BoolVal *const result,
                              const tpl::sql::Integer *const left,
                              const tpl::sql::Integer *const right) {
  tpl::sql::ComparisonFunctions::EqInteger(result, *left, *right);
}

VM_OP_HOT void OpLessThanInteger(tpl::sql::BoolVal *const result,
                                 const tpl::sql::Integer *const left,
                                 const tpl::sql::Integer *const right) {
  tpl::sql::ComparisonFunctions::LtInteger(result, *left, *right);
}

VM_OP_HOT void OpLessThanEqualInteger(tpl::sql::BoolVal *const result,
                                      const tpl::sql::Integer *const left,
                                      const tpl::sql::Integer *const right) {
  tpl::sql::ComparisonFunctions::LeInteger(result, *left, *right);
}

VM_OP_HOT void OpNotEqualInteger(tpl::sql::BoolVal *const result,
                                 const tpl::sql::Integer *const left,
                                 const tpl::sql::Integer *const right) {
  tpl::sql::ComparisonFunctions::NeInteger(result, *left, *right);
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

VM_OP_HOT void OpAggregationHashTableLookup(
    byte **result, tpl::sql::AggregationHashTable *const agg_hash_table,
    const hash_t hash_val,
    const tpl::sql::AggregationHashTable::KeyEqFn key_eq_fn,
    tpl::sql::VectorProjectionIterator *iters[]) {
  *result = agg_hash_table->Lookup(hash_val, key_eq_fn, iters);
}

VM_OP_HOT void OpAggregationHashTableProcessBatch(
    tpl::sql::AggregationHashTable *const agg_hash_table,
    tpl::sql::VectorProjectionIterator *iters[],
    const tpl::sql::AggregationHashTable::HashFn hash_fn,
    const tpl::sql::AggregationHashTable::KeyEqFn key_eq_fn,
    const tpl::sql::AggregationHashTable::InitAggFn init_agg_fn,
    const tpl::sql::AggregationHashTable::AdvanceAggFn merge_agg_fn) {
  agg_hash_table->ProcessBatch(iters, hash_fn, key_eq_fn, init_agg_fn,
                               merge_agg_fn);
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
    tpl::sql::AggregationHashTableIterator *iter,
    tpl::sql::AggregationHashTable *agg_hash_table);

VM_OP_HOT void OpAggregationHashTableIteratorHasNext(
    bool *has_more, tpl::sql::AggregationHashTableIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationHashTableIteratorNext(
    tpl::sql::AggregationHashTableIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpAggregationHashTableIteratorGetRow(
    const byte **row, tpl::sql::AggregationHashTableIterator *iter) {
  *row = iter->GetCurrentAggregateRow();
}

void OpAggregationHashTableIteratorFree(
    tpl::sql::AggregationHashTableIterator *iter);

VM_OP_HOT void OpAggregationOverflowPartitionIteratorHasNext(
    bool *has_more, tpl::sql::AggregationOverflowPartitionIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorNext(
    tpl::sql::AggregationOverflowPartitionIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetHash(
    hash_t *hash_val, tpl::sql::AggregationOverflowPartitionIterator *iter) {
  *hash_val = iter->GetHash();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRow(
    const byte **row, tpl::sql::AggregationOverflowPartitionIterator *iter) {
  *row = iter->GetPayload();
}

//
// COUNT
//

VM_OP_HOT void OpCountAggregateInit(tpl::sql::CountAggregate *agg) {
  new (agg) tpl::sql::CountAggregate();
}

VM_OP_HOT void OpCountAggregateAdvance(tpl::sql::CountAggregate *agg,
                                       tpl::sql::Val *val) {
  agg->Advance(val);
}

VM_OP_HOT void OpCountAggregateMerge(tpl::sql::CountAggregate *agg_1,
                                     tpl::sql::CountAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountAggregateReset(tpl::sql::CountAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpCountAggregateGetResult(tpl::sql::Integer *result,
                                         tpl::sql::CountAggregate *agg) {
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
                                           tpl::sql::Val *val) {
  agg->Advance(val);
}

VM_OP_HOT void OpCountStarAggregateMerge(tpl::sql::CountStarAggregate *agg_1,
                                         tpl::sql::CountStarAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountStarAggregateReset(tpl::sql::CountStarAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpCountStarAggregateGetResult(
    tpl::sql::Integer *result, tpl::sql::CountStarAggregate *agg) {
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
                                            tpl::sql::Integer *val) {
  agg->Advance(val);
}

VM_OP_HOT void OpIntegerSumAggregateAdvanceNullable(
    tpl::sql::IntegerSumAggregate *agg, tpl::sql::Integer *val) {
  agg->AdvanceNullable(val);
}

VM_OP_HOT void OpIntegerSumAggregateMerge(
    tpl::sql::IntegerSumAggregate *agg_1,
    tpl::sql::IntegerSumAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerSumAggregateReset(tpl::sql::IntegerSumAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpIntegerSumAggregateGetResult(
    tpl::sql::Integer *result, tpl::sql::IntegerSumAggregate *agg) {
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
                                            tpl::sql::Integer *val) {
  agg->Advance(val);
}

VM_OP_HOT void OpIntegerMaxAggregateAdvanceNullable(
    tpl::sql::IntegerMaxAggregate *agg, tpl::sql::Integer *val) {
  agg->AdvanceNullable(val);
}

VM_OP_HOT void OpIntegerMaxAggregateMerge(
    tpl::sql::IntegerMaxAggregate *agg_1,
    tpl::sql::IntegerMaxAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMaxAggregateReset(tpl::sql::IntegerMaxAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpIntegerMaxAggregateGetResult(
    tpl::sql::Integer *result, tpl::sql::IntegerMaxAggregate *agg) {
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
                                            tpl::sql::Integer *val) {
  agg->Advance(val);
}

VM_OP_HOT void OpIntegerMinAggregateAdvanceNullable(
    tpl::sql::IntegerMinAggregate *agg, tpl::sql::Integer *val) {
  agg->AdvanceNullable(val);
}

VM_OP_HOT void OpIntegerMinAggregateMerge(
    tpl::sql::IntegerMinAggregate *agg_1,
    tpl::sql::IntegerMinAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMinAggregateReset(tpl::sql::IntegerMinAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpIntegerMinAggregateGetResult(
    tpl::sql::Integer *result, tpl::sql::IntegerMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpIntegerMinAggregateFree(tpl::sql::IntegerMinAggregate *agg) {
  agg->~IntegerMinAggregate();
}

//
// AVG(int_type)
//

VM_OP_HOT void OpIntegerAvgAggregateInit(tpl::sql::IntegerAvgAggregate *agg) {
  new (agg) tpl::sql::IntegerAvgAggregate();
}

VM_OP_HOT void OpIntegerAvgAggregateAdvance(tpl::sql::IntegerAvgAggregate *agg,
                                            tpl::sql::Integer *val) {
  agg->Advance(val);
}

VM_OP_HOT void OpIntegerAvgAggregateAdvanceNullable(
    tpl::sql::IntegerAvgAggregate *agg, tpl::sql::Integer *val) {
  agg->AdvanceNullable(val);
}

VM_OP_HOT void OpIntegerAvgAggregateMerge(
    tpl::sql::IntegerAvgAggregate *agg_1,
    tpl::sql::IntegerAvgAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerAvgAggregateReset(tpl::sql::IntegerAvgAggregate *agg) {
  agg->Reset();
}

VM_OP_HOT void OpIntegerAvgAggregateGetResult(
    tpl::sql::Integer *result, tpl::sql::IntegerAvgAggregate *agg) {
  *result = agg->GetResultAvg();
}

VM_OP_HOT void OpIntegerAvgAggregateFree(tpl::sql::IntegerAvgAggregate *agg) {
  agg->~IntegerAvgAggregate();
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

VM_OP_HOT void OpAcos(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Acos(result, *input);
}

VM_OP_HOT void OpAsin(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Asin(result, *input);
}

VM_OP_HOT void OpAtan(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Atan(result, *input);
}

VM_OP_HOT void OpAtan2(tpl::sql::Real *result, const tpl::sql::Real *arg_1,
                       const tpl::sql::Real *arg_2) {
  tpl::sql::ArithmeticFunctions::Atan2(result, *arg_1, *arg_2);
}

VM_OP_HOT void OpCos(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Cos(result, *input);
}

VM_OP_HOT void OpCot(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Cot(result, *input);
}

VM_OP_HOT void OpSin(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Sin(result, *input);
}

VM_OP_HOT void OpTan(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Tan(result, *input);
}

}  // extern "C"
