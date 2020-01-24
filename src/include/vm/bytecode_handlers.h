#pragma once

#include <cstdint>

#include "common/common.h"
#include "common/macros.h"
#include "sql/aggregation_hash_table.h"
#include "sql/aggregators.h"
#include "sql/execution_context.h"
#include "sql/filter_manager.h"
#include "sql/functions/arithmetic_functions.h"
#include "sql/functions/casting_fuctions.h"
#include "sql/functions/comparison_functions.h"
#include "sql/functions/is_null_predicate.h"
#include "sql/functions/string_functions.h"
#include "sql/join_hash_table.h"
#include "sql/join_hash_table_vector_probe.h"
#include "sql/operations/hash_operators.h"
#include "sql/sorter.h"
#include "sql/table_vector_iterator.h"
#include "sql/thread_state_container.h"
#include "sql/vector_filter_executor.h"

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

#define COMPARISONS(type, ...)                                                                    \
  /* Primitive greater-than-equal implementation */                                               \
  VM_OP_HOT void OpGreaterThanEqual##_##type(bool *result, type lhs, type rhs) {                  \
    *result = (lhs >= rhs);                                                                       \
  }                                                                                               \
                                                                                                  \
  /* Primitive greater-than implementation */                                                     \
  VM_OP_HOT void OpGreaterThan##_##type(bool *result, type lhs, type rhs) {                       \
    *result = (lhs > rhs);                                                                        \
  }                                                                                               \
                                                                                                  \
  /* Primitive equal-to implementation */                                                         \
  VM_OP_HOT void OpEqual##_##type(bool *result, type lhs, type rhs) { *result = (lhs == rhs); }   \
                                                                                                  \
  /* Primitive less-than-equal implementation */                                                  \
  VM_OP_HOT void OpLessThanEqual##_##type(bool *result, type lhs, type rhs) {                     \
    *result = (lhs <= rhs);                                                                       \
  }                                                                                               \
                                                                                                  \
  /* Primitive less-than implementation */                                                        \
  VM_OP_HOT void OpLessThan##_##type(bool *result, type lhs, type rhs) { *result = (lhs < rhs); } \
                                                                                                  \
  /* Primitive not-equal-to implementation */                                                     \
  VM_OP_HOT void OpNotEqual##_##type(bool *result, type lhs, type rhs) { *result = (lhs != rhs); }

ALL_TYPES(COMPARISONS);

#undef COMPARISONS

VM_OP_HOT void OpNot(bool *const result, const bool input) { *result = !input; }

// ---------------------------------------------------------
// Primitive arithmetic
// ---------------------------------------------------------

#define ARITHMETIC(type, ...)                                                              \
  /* Primitive addition */                                                                 \
  VM_OP_HOT void OpAdd##_##type(type *result, type lhs, type rhs) { *result = lhs + rhs; } \
                                                                                           \
  /* Primitive subtraction */                                                              \
  VM_OP_HOT void OpSub##_##type(type *result, type lhs, type rhs) { *result = lhs - rhs; } \
                                                                                           \
  /* Primitive multiplication */                                                           \
  VM_OP_HOT void OpMul##_##type(type *result, type lhs, type rhs) { *result = lhs * rhs; } \
                                                                                           \
  /* Primitive negation */                                                                 \
  VM_OP_HOT void OpNeg##_##type(type *result, type input) { *result = -input; }            \
                                                                                           \
  /* Primitive division (no zero-check) */                                                 \
  VM_OP_HOT void OpDiv##_##type(type *result, type lhs, type rhs) {                        \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");                                       \
    *result = lhs / rhs;                                                                   \
  }

ALL_NUMERIC_TYPES(ARITHMETIC);

#undef ARITHMETIC

#define INT_MODULAR(type, ...)                                      \
  /* Primitive modulo-remainder (no zero-check) */                  \
  VM_OP_HOT void OpRem##_##type(type *result, type lhs, type rhs) { \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");                \
    *result = lhs % rhs;                                            \
  }

#define FLOAT_MODULAR(type, ...)                                    \
  /* Primitive modulo-remainder (no zero-check) */                  \
  VM_OP_HOT void OpRem##_##type(type *result, type lhs, type rhs) { \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");                \
    *result = std::fmod(lhs, rhs);                                  \
  }

INT_TYPES(INT_MODULAR)
FLOAT_TYPES(FLOAT_MODULAR)

#undef FLOAT_MODULAR
#undef INT_MODULAR

// ---------------------------------------------------------
// Bitwise operations
// ---------------------------------------------------------

#define BITS(type, ...)                                                                         \
  /* Primitive bitwise AND */                                                                   \
  VM_OP_HOT void OpBitAnd##_##type(type *result, type lhs, type rhs) { *result = (lhs & rhs); } \
                                                                                                \
  /* Primitive bitwise OR */                                                                    \
  VM_OP_HOT void OpBitOr##_##type(type *result, type lhs, type rhs) { *result = (lhs | rhs); }  \
                                                                                                \
  /* Primitive bitwise XOR */                                                                   \
  VM_OP_HOT void OpBitXor##_##type(type *result, type lhs, type rhs) { *result = (lhs ^ rhs); } \
                                                                                                \
  /* Primitive bitwise COMPLEMENT */                                                            \
  VM_OP_HOT void OpBitNeg##_##type(type *result, type input) { *result = ~input; }

INT_TYPES(BITS);

#undef BITS

// ---------------------------------------------------------
// Memory operations
// ---------------------------------------------------------

VM_OP_HOT void OpIsNullPtr(bool *result, const void *const ptr) { *result = (ptr == nullptr); }

VM_OP_HOT void OpIsNotNullPtr(bool *result, const void *const ptr) { *result = (ptr != nullptr); }

VM_OP_HOT void OpDeref1(int8_t *dest, const int8_t *const src) { *dest = *src; }

VM_OP_HOT void OpDeref2(int16_t *dest, const int16_t *const src) { *dest = *src; }

VM_OP_HOT void OpDeref4(int32_t *dest, const int32_t *const src) { *dest = *src; }

VM_OP_HOT void OpDeref8(int64_t *dest, const int64_t *const src) { *dest = *src; }

VM_OP_HOT void OpDerefN(byte *dest, const byte *const src, uint32_t len) {
  std::memcpy(dest, src, len);
}

VM_OP_HOT void OpAssign1(int8_t *dest, int8_t src) { *dest = src; }

VM_OP_HOT void OpAssign2(int16_t *dest, int16_t src) { *dest = src; }

VM_OP_HOT void OpAssign4(int32_t *dest, int32_t src) { *dest = src; }

VM_OP_HOT void OpAssign8(int64_t *dest, int64_t src) { *dest = src; }

VM_OP_HOT void OpAssignImm1(int8_t *dest, int8_t src) { *dest = src; }

VM_OP_HOT void OpAssignImm2(int16_t *dest, int16_t src) { *dest = src; }

VM_OP_HOT void OpAssignImm4(int32_t *dest, int32_t src) { *dest = src; }

VM_OP_HOT void OpAssignImm8(int64_t *dest, int64_t src) { *dest = src; }

VM_OP_HOT void OpAssignImm4F(float *dest, float src) { *dest = src; }

VM_OP_HOT void OpAssignImm8F(double *dest, double src) { *dest = src; }

VM_OP_HOT void OpLea(byte **dest, byte *base, uint32_t offset) { *dest = base + offset; }

VM_OP_HOT void OpLeaScaled(byte **dest, byte *base, uint32_t index, uint32_t scale,
                           uint32_t offset) {
  *dest = base + (scale * index) + offset;
}

VM_OP_HOT bool OpJump() { return true; }

VM_OP_HOT bool OpJumpIfTrue(bool cond) { return cond; }

VM_OP_HOT bool OpJumpIfFalse(bool cond) { return !cond; }

VM_OP_HOT void OpCall(UNUSED uint16_t func_id, UNUSED uint16_t num_args) {}

VM_OP_HOT void OpReturn() {}

// ---------------------------------------------------------
// Execution Context
// ---------------------------------------------------------

VM_OP_WARM void OpExecutionContextGetMemoryPool(tpl::sql::MemoryPool **const memory,
                                                tpl::sql::ExecutionContext *const exec_ctx) {
  *memory = exec_ctx->GetMemoryPool();
}

VM_OP_WARM void OpExecutionContextGetTLS(
    tpl::sql::ThreadStateContainer **const thread_state_container,
    tpl::sql::ExecutionContext *const exec_ctx) {
  *thread_state_container = exec_ctx->GetThreadStateContainer();
}

VM_OP void OpThreadStateContainerInit(tpl::sql::ThreadStateContainer *thread_state_container,
                                      tpl::sql::MemoryPool *memory);

VM_OP_HOT void OpThreadStateContainerReset(tpl::sql::ThreadStateContainer *thread_state_container,
                                           uint32_t size,
                                           tpl::sql::ThreadStateContainer::InitFn init_fn,
                                           tpl::sql::ThreadStateContainer::DestroyFn destroy_fn,
                                           void *ctx) {
  thread_state_container->Reset(size, init_fn, destroy_fn, ctx);
}

VM_OP_HOT void OpThreadStateContainerIterate(tpl::sql::ThreadStateContainer *thread_state_container,
                                             void *const state,
                                             tpl::sql::ThreadStateContainer::IterateFn iterate_fn) {
  thread_state_container->IterateStates(state, iterate_fn);
}

VM_OP void OpThreadStateContainerFree(tpl::sql::ThreadStateContainer *thread_state_container);

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

VM_OP void OpTableVectorIteratorInit(tpl::sql::TableVectorIterator *iter, uint16_t table_id);

VM_OP void OpTableVectorIteratorPerformInit(tpl::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorNext(bool *has_more, tpl::sql::TableVectorIterator *iter) {
  *has_more = iter->Advance();
}

VM_OP void OpTableVectorIteratorFree(tpl::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorGetVPI(tpl::sql::VectorProjectionIterator **vpi,
                                           tpl::sql::TableVectorIterator *iter) {
  *vpi = iter->GetVectorProjectionIterator();
}

VM_OP_HOT void OpParallelScanTable(const uint16_t table_id, void *const query_state,
                                   tpl::sql::ThreadStateContainer *const thread_states,
                                   const tpl::sql::TableVectorIterator::ScanFn scanner) {
  tpl::sql::TableVectorIterator::ParallelScan(table_id, query_state, thread_states, scanner);
}

// ---------------------------------------------------------
// Vector Projection Iterator
// ---------------------------------------------------------

void OpVPIInit(tpl::sql::VectorProjectionIterator *vpi, tpl::sql::VectorProjection *vp);

void OpVPIInitWithList(tpl::sql::VectorProjectionIterator *vpi, tpl::sql::VectorProjection *vp,
                       tpl::sql::TupleIdList *tid_list);

void OpVPIFree(tpl::sql::VectorProjectionIterator *vpi);

VM_OP_HOT void OpVPIIsFiltered(bool *is_filtered, const tpl::sql::VectorProjectionIterator *vpi) {
  *is_filtered = vpi->IsFiltered();
}

VM_OP_HOT void OpVPIGetSelectedRowCount(uint32_t *count,
                                        const tpl::sql::VectorProjectionIterator *vpi) {
  *count = vpi->GetSelectedTupleCount();
}

VM_OP_HOT void OpVPIGetVectorProjection(tpl::sql::VectorProjection **vector_projection,
                                        const tpl::sql::VectorProjectionIterator *vpi) {
  *vector_projection = vpi->GetVectorProjection();
}

VM_OP_HOT void OpVPIHasNext(bool *has_more, const tpl::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNext();
}

VM_OP_HOT void OpVPIHasNextFiltered(bool *has_more, const tpl::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNextFiltered();
}

VM_OP_HOT void OpVPIAdvance(tpl::sql::VectorProjectionIterator *vpi) { vpi->Advance(); }

VM_OP_HOT void OpVPIAdvanceFiltered(tpl::sql::VectorProjectionIterator *vpi) {
  vpi->AdvanceFiltered();
}

VM_OP_HOT void OpVPISetPosition(tpl::sql::VectorProjectionIterator *vpi, const uint32_t index) {
  vpi->SetPosition<false>(index);
}

VM_OP_HOT void OpVPISetPositionFiltered(tpl::sql::VectorProjectionIterator *const vpi,
                                        const uint32_t index) {
  vpi->SetPosition<true>(index);
}

VM_OP_HOT void OpVPIMatch(tpl::sql::VectorProjectionIterator *vpi, const bool match) {
  vpi->Match(match);
}

VM_OP_HOT void OpVPIReset(tpl::sql::VectorProjectionIterator *vpi) { vpi->Reset(); }

VM_OP_HOT void OpVPIResetFiltered(tpl::sql::VectorProjectionIterator *vpi) { vpi->ResetFiltered(); }

// ---------------------------------------------------------
// VPI Get
// ---------------------------------------------------------

#define GEN_VPI_GET(Name, SqlValueType, CppType)                                     \
  VM_OP_HOT void OpVPIGet##Name(tpl::sql::SqlValueType *out,                         \
                                tpl::sql::VectorProjectionIterator *const vpi,       \
                                const uint32_t col_idx) {                            \
    auto *ptr = vpi->GetValue<CppType, false>(col_idx, nullptr);                     \
    TPL_ASSERT(ptr != nullptr, "Null data pointer when trying to read attribute");   \
    out->is_null = false;                                                            \
    out->val = *ptr;                                                                 \
  }                                                                                  \
  VM_OP_HOT void OpVPIGet##Name##Null(tpl::sql::SqlValueType *out,                   \
                                      tpl::sql::VectorProjectionIterator *const vpi, \
                                      const uint32_t col_idx) {                      \
    bool null = false;                                                               \
    auto *ptr = vpi->GetValue<CppType, true>(col_idx, &null);                        \
    TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");          \
    out->is_null = null;                                                             \
    out->val = *ptr;                                                                 \
  }

#define GEN_VPI_SET(Name, SqlValueType, CppType)                                               \
  VM_OP_HOT void OpVPISet##Name(tpl::sql::VectorProjectionIterator *const vpi,                 \
                                tpl::sql::SqlValueType *input, const uint32_t col_idx) {       \
    vpi->SetValue<CppType, false>(col_idx, input->val, false);                                 \
  }                                                                                            \
  VM_OP_HOT void OpVPISet##Name##Null(tpl::sql::VectorProjectionIterator *const vpi,           \
                                      tpl::sql::SqlValueType *input, const uint32_t col_idx) { \
    vpi->SetValue<CppType, true>(col_idx, input->val, input->is_null);                         \
  }

GEN_VPI_GET(Bool, BoolVal, bool);
GEN_VPI_GET(TinyInt, Integer, int8_t);
GEN_VPI_GET(SmallInt, Integer, int16_t);
GEN_VPI_GET(Integer, Integer, int32_t);
GEN_VPI_GET(BigInt, Integer, int64_t);
GEN_VPI_GET(Real, Real, float);
GEN_VPI_GET(Double, Real, double);
GEN_VPI_GET(Decimal, DecimalVal, tpl::sql::Decimal64);
GEN_VPI_GET(Date, DateVal, tpl::sql::Date);
GEN_VPI_GET(String, StringVal, tpl::sql::VarlenEntry);

GEN_VPI_SET(Bool, BoolVal, bool);
GEN_VPI_SET(TinyInt, Integer, int8_t);
GEN_VPI_SET(SmallInt, Integer, int16_t);
GEN_VPI_SET(Integer, Integer, int32_t);
GEN_VPI_SET(BigInt, Integer, int64_t);
GEN_VPI_SET(Real, Real, float);
GEN_VPI_SET(Double, Real, double);
GEN_VPI_SET(Decimal, DecimalVal, tpl::sql::Decimal64);
GEN_VPI_SET(Date, DateVal, tpl::sql::Date);
GEN_VPI_SET(String, StringVal, tpl::sql::VarlenEntry);

#undef GEN_VPI_SET
#undef GEN_VPI_GET

// ---------------------------------------------------------
// Hashing
// ---------------------------------------------------------

VM_OP_HOT void OpHashInt(hash_t *const hash_val, const tpl::sql::Integer *const input,
                         const hash_t seed) {
  *hash_val = input->is_null ? 0 : tpl::util::HashUtil::HashCrc(input->val, seed);
}

VM_OP_HOT void OpHashReal(hash_t *const hash_val, const tpl::sql::Real *const input,
                          const hash_t seed) {
  *hash_val = input->is_null ? 0 : tpl::util::HashUtil::HashCrc(input->val, seed);
}

VM_OP_HOT void OpHashString(hash_t *const hash_val, const tpl::sql::StringVal *const input,
                            const hash_t seed) {
  *hash_val = input->is_null ? 0 : input->val.Hash(seed);
}

VM_OP_HOT void OpHashDate(hash_t *const hash_val, const tpl::sql::DateVal *const input,
                          const hash_t seed) {
  *hash_val = input->is_null ? 0 : input->val.Hash(seed);
}

VM_OP_HOT void OpHashCombine(hash_t *hash_val, hash_t new_hash_val) {
  *hash_val = tpl::util::HashUtil::CombineHashes(*hash_val, new_hash_val);
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

VM_OP void OpFilterManagerInit(tpl::sql::FilterManager *filter_manager);

VM_OP void OpFilterManagerStartNewClause(tpl::sql::FilterManager *filter_manager);

VM_OP void OpFilterManagerInsertFilter(tpl::sql::FilterManager *filter_manager,
                                       tpl::sql::FilterManager::MatchFn flavor);

VM_OP void OpFilterManagerFinalize(tpl::sql::FilterManager *filter_manager);

VM_OP void OpFilterManagerRunFilters(tpl::sql::FilterManager *filter,
                                     tpl::sql::VectorProjectionIterator *vpi);

VM_OP void OpFilterManagerFree(tpl::sql::FilterManager *filter);

// ---------------------------------------------------------
// Vector Filter Executor
// ---------------------------------------------------------

#define GEN_VECTOR_FILTER(Name)                                                                   \
  VM_OP_HOT void OpVectorFilter##Name(tpl::sql::VectorProjection *vector_projection,              \
                                      const uint32_t left_col_idx, const uint32_t right_col_idx,  \
                                      tpl::sql::TupleIdList *tid_list) {                          \
    tpl::sql::VectorFilterExecutor::Select##Name(vector_projection, left_col_idx, right_col_idx,  \
                                                 tid_list);                                       \
  }                                                                                               \
  VM_OP_HOT void OpVectorFilter##Name##Val(tpl::sql::VectorProjection *vector_projection,         \
                                           const uint32_t left_col_idx, const tpl::sql::Val *val, \
                                           tpl::sql::TupleIdList *tid_list) {                     \
    tpl::sql::VectorFilterExecutor::Select##Name##Val(vector_projection, left_col_idx, *val,      \
                                                      tid_list);                                  \
  }

GEN_VECTOR_FILTER(Equal)
GEN_VECTOR_FILTER(GreaterThan)
GEN_VECTOR_FILTER(GreaterThanEqual)
GEN_VECTOR_FILTER(LessThan)
GEN_VECTOR_FILTER(LessThanEqual)
GEN_VECTOR_FILTER(NotEqual)

#undef GEN_VECTOR_FILTER

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

VM_OP_HOT void OpInitInteger(tpl::sql::Integer *result, int32_t input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitReal(tpl::sql::Real *result, float input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitDate(tpl::sql::DateVal *result, uint32_t year, uint32_t month, uint32_t day) {
  result->is_null = false;
  result->val = tpl::sql::Date::FromYMD(year, month, day);
}

VM_OP_HOT void OpInitString(tpl::sql::StringVal *result, const uint8_t *str, uint32_t length) {
  result->is_null = false;
  result->val = tpl::sql::VarlenEntry::Create(reinterpret_cast<const byte *>(str), length);
}

VM_OP_HOT void OpIntegerToReal(tpl::sql::Real *result, const tpl::sql::Integer *input) {
  tpl::sql::CastingFunctions::CastToReal(result, *input);
}

VM_OP_HOT void OpRealToInteger(tpl::sql::Integer *result, const tpl::sql::Real *input) {
  tpl::sql::CastingFunctions::CastToInteger(result, *input);
}

#define GEN_SQL_COMPARISONS(NAME, TYPE)                                                           \
  VM_OP_HOT void OpGreaterThan##NAME(tpl::sql::BoolVal *const result,                             \
                                     const tpl::sql::TYPE *const left,                            \
                                     const tpl::sql::TYPE *const right) {                         \
    tpl::sql::ComparisonFunctions::Gt##TYPE(result, *left, *right);                               \
  }                                                                                               \
  VM_OP_HOT void OpGreaterThanEqual##NAME(tpl::sql::BoolVal *const result,                        \
                                          const tpl::sql::TYPE *const left,                       \
                                          const tpl::sql::TYPE *const right) {                    \
    tpl::sql::ComparisonFunctions::Ge##TYPE(result, *left, *right);                               \
  }                                                                                               \
  VM_OP_HOT void OpEqual##NAME(tpl::sql::BoolVal *const result, const tpl::sql::TYPE *const left, \
                               const tpl::sql::TYPE *const right) {                               \
    tpl::sql::ComparisonFunctions::Eq##TYPE(result, *left, *right);                               \
  }                                                                                               \
  VM_OP_HOT void OpLessThan##NAME(tpl::sql::BoolVal *const result,                                \
                                  const tpl::sql::TYPE *const left,                               \
                                  const tpl::sql::TYPE *const right) {                            \
    tpl::sql::ComparisonFunctions::Lt##TYPE(result, *left, *right);                               \
  }                                                                                               \
  VM_OP_HOT void OpLessThanEqual##NAME(tpl::sql::BoolVal *const result,                           \
                                       const tpl::sql::TYPE *const left,                          \
                                       const tpl::sql::TYPE *const right) {                       \
    tpl::sql::ComparisonFunctions::Le##TYPE(result, *left, *right);                               \
  }                                                                                               \
  VM_OP_HOT void OpNotEqual##NAME(tpl::sql::BoolVal *const result,                                \
                                  const tpl::sql::TYPE *const left,                               \
                                  const tpl::sql::TYPE *const right) {                            \
    tpl::sql::ComparisonFunctions::Ne##TYPE(result, *left, *right);                               \
  }

GEN_SQL_COMPARISONS(Bool, BoolVal)
GEN_SQL_COMPARISONS(Integer, Integer)
GEN_SQL_COMPARISONS(Real, Real)
GEN_SQL_COMPARISONS(Date, DateVal)
GEN_SQL_COMPARISONS(String, StringVal)

#undef GEN_SQL_COMPARISONS

VM_OP_WARM void OpAbsInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left) {
  tpl::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_WARM void OpAbsReal(tpl::sql::Real *const result, const tpl::sql::Real *const left) {
  tpl::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_HOT void OpAddInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool overflow;
  tpl::sql::ArithmeticFunctions::Add(result, *left, *right, &overflow);
}

VM_OP_HOT void OpSubInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool overflow;
  tpl::sql::ArithmeticFunctions::Sub(result, *left, *right, &overflow);
}

VM_OP_HOT void OpMulInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool overflow;
  tpl::sql::ArithmeticFunctions::Mul(result, *left, *right, &overflow);
}

VM_OP_HOT void OpDivInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::IntDiv(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpRemInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::IntMod(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpAddReal(tpl::sql::Real *const result, const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  tpl::sql::ArithmeticFunctions::Add(result, *left, *right);
}

VM_OP_HOT void OpSubReal(tpl::sql::Real *const result, const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  tpl::sql::ArithmeticFunctions::Sub(result, *left, *right);
}

VM_OP_HOT void OpMulReal(tpl::sql::Real *const result, const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  tpl::sql::ArithmeticFunctions::Mul(result, *left, *right);
}

VM_OP_HOT void OpDivReal(tpl::sql::Real *const result, const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::Div(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpRemReal(tpl::sql::Real *const result, const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::Mod(result, *left, *right, &div_by_zero);
}

// ---------------------------------------------------------
// SQL Aggregations
// ---------------------------------------------------------

VM_OP void OpAggregationHashTableInit(tpl::sql::AggregationHashTable *agg_hash_table,
                                      tpl::sql::MemoryPool *memory, uint32_t payload_size);

VM_OP_HOT void OpAggregationHashTableAllocTuple(byte **result,
                                                tpl::sql::AggregationHashTable *agg_hash_table,
                                                const hash_t hash_val) {
  *result = agg_hash_table->AllocInputTuple(hash_val);
}

VM_OP_HOT void OpAggregationHashTableAllocTuplePartitioned(
    byte **result, tpl::sql::AggregationHashTable *agg_hash_table, const hash_t hash_val) {
  *result = agg_hash_table->AllocInputTuplePartitioned(hash_val);
}

VM_OP_HOT void OpAggregationHashTableLinkHashTableEntry(
    tpl::sql::AggregationHashTable *agg_hash_table, tpl::sql::HashTableEntry *entry) {
  agg_hash_table->Insert(entry);
}

VM_OP_HOT void OpAggregationHashTableLookup(byte **result,
                                            tpl::sql::AggregationHashTable *const agg_hash_table,
                                            const hash_t hash_val,
                                            const tpl::sql::AggregationHashTable::KeyEqFn key_eq_fn,
                                            const void *probe_tuple) {
  *result = agg_hash_table->Lookup(hash_val, key_eq_fn, probe_tuple);
}

VM_OP_HOT void OpAggregationHashTableProcessBatch(
    tpl::sql::AggregationHashTable *const agg_hash_table, tpl::sql::VectorProjectionIterator *vpi,
    const tpl::sql::AggregationHashTable::HashFn hash_fn,
    const tpl::sql::AggregationHashTable::KeyEqFn key_eq_fn,
    const tpl::sql::AggregationHashTable::VectorInitAggFn init_agg_fn,
    const tpl::sql::AggregationHashTable::VectorAdvanceAggFn merge_agg_fn, const bool partitioned) {
  // agg_hash_table->ProcessBatch(vpi, hash_fn, key_eq_fn, init_agg_fn, merge_agg_fn, partitioned);
}

VM_OP_HOT void OpAggregationHashTableTransferPartitions(
    tpl::sql::AggregationHashTable *const agg_hash_table,
    tpl::sql::ThreadStateContainer *const thread_state_container, const uint32_t agg_ht_offset,
    const tpl::sql::AggregationHashTable::MergePartitionFn merge_partition_fn) {
  agg_hash_table->TransferMemoryAndPartitions(thread_state_container, agg_ht_offset,
                                              merge_partition_fn);
}

VM_OP_HOT void OpAggregationHashTableParallelPartitionedScan(
    tpl::sql::AggregationHashTable *const agg_hash_table, void *const query_state,
    tpl::sql::ThreadStateContainer *const thread_state_container,
    const tpl::sql::AggregationHashTable::ScanPartitionFn scan_partition_fn) {
  agg_hash_table->ExecuteParallelPartitionedScan(query_state, thread_state_container,
                                                 scan_partition_fn);
}

VM_OP void OpAggregationHashTableFree(tpl::sql::AggregationHashTable *agg_hash_table);

VM_OP void OpAggregationHashTableIteratorInit(tpl::sql::AHTIterator *iter,
                                              tpl::sql::AggregationHashTable *agg_hash_table);

VM_OP_HOT void OpAggregationHashTableIteratorHasNext(bool *has_more, tpl::sql::AHTIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationHashTableIteratorNext(tpl::sql::AHTIterator *iter) { iter->Next(); }

VM_OP_HOT void OpAggregationHashTableIteratorGetRow(const byte **row, tpl::sql::AHTIterator *iter) {
  *row = iter->GetCurrentAggregateRow();
}

VM_OP void OpAggregationHashTableIteratorFree(tpl::sql::AHTIterator *iter);

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
  *hash_val = iter->GetRowHash();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRow(
    const byte **row, tpl::sql::AHTOverflowPartitionIterator *iter) {
  *row = iter->GetRow();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRowEntry(
    tpl::sql::HashTableEntry **entry, tpl::sql::AHTOverflowPartitionIterator *iter) {
  *entry = iter->GetEntryForRow();
}

// ---------------------------------------------------------
// COUNT
// ---------------------------------------------------------

VM_OP_HOT void OpCountAggregateInit(tpl::sql::CountAggregate *agg) {
  new (agg) tpl::sql::CountAggregate();
}

VM_OP_HOT void OpCountAggregateAdvance(tpl::sql::CountAggregate *agg, const tpl::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountAggregateMerge(tpl::sql::CountAggregate *agg_1,
                                     const tpl::sql::CountAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountAggregateReset(tpl::sql::CountAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountAggregateGetResult(tpl::sql::Integer *result,
                                         const tpl::sql::CountAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountAggregateFree(tpl::sql::CountAggregate *agg) { agg->~CountAggregate(); }

// ---------------------------------------------------------
// COUNT(*)
// ---------------------------------------------------------

VM_OP_HOT void OpCountStarAggregateInit(tpl::sql::CountStarAggregate *agg) {
  new (agg) tpl::sql::CountStarAggregate();
}

VM_OP_HOT void OpCountStarAggregateAdvance(tpl::sql::CountStarAggregate *agg,
                                           const tpl::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountStarAggregateMerge(tpl::sql::CountStarAggregate *agg_1,
                                         const tpl::sql::CountStarAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountStarAggregateReset(tpl::sql::CountStarAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountStarAggregateGetResult(tpl::sql::Integer *result,
                                             const tpl::sql::CountStarAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountStarAggregateFree(tpl::sql::CountStarAggregate *agg) {
  agg->~CountStarAggregate();
}

// ---------------------------------------------------------
// SUM
// ---------------------------------------------------------

VM_OP_HOT void OpIntegerSumAggregateInit(tpl::sql::IntegerSumAggregate *agg) {
  new (agg) tpl::sql::IntegerSumAggregate();
}

VM_OP_HOT void OpIntegerSumAggregateAdvance(tpl::sql::IntegerSumAggregate *agg,
                                            const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerSumAggregateMerge(tpl::sql::IntegerSumAggregate *agg_1,
                                          const tpl::sql::IntegerSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerSumAggregateReset(tpl::sql::IntegerSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerSumAggregateGetResult(tpl::sql::Integer *result,
                                              const tpl::sql::IntegerSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpIntegerSumAggregateFree(tpl::sql::IntegerSumAggregate *agg) {
  agg->~IntegerSumAggregate();
}

VM_OP_HOT void OpRealSumAggregateInit(tpl::sql::RealSumAggregate *agg) {
  new (agg) tpl::sql::RealSumAggregate();
}

VM_OP_HOT void OpRealSumAggregateAdvance(tpl::sql::RealSumAggregate *agg,
                                         const tpl::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealSumAggregateMerge(tpl::sql::RealSumAggregate *agg_1,
                                       const tpl::sql::RealSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealSumAggregateReset(tpl::sql::RealSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealSumAggregateGetResult(tpl::sql::Real *result,
                                           const tpl::sql::RealSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpRealSumAggregateFree(tpl::sql::RealSumAggregate *agg) { agg->~RealSumAggregate(); }

// ---------------------------------------------------------
// MAX
// ---------------------------------------------------------

VM_OP_HOT void OpIntegerMaxAggregateInit(tpl::sql::IntegerMaxAggregate *agg) {
  new (agg) tpl::sql::IntegerMaxAggregate();
}

VM_OP_HOT void OpIntegerMaxAggregateAdvance(tpl::sql::IntegerMaxAggregate *agg,
                                            const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMaxAggregateMerge(tpl::sql::IntegerMaxAggregate *agg_1,
                                          const tpl::sql::IntegerMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMaxAggregateReset(tpl::sql::IntegerMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMaxAggregateGetResult(tpl::sql::Integer *result,
                                              const tpl::sql::IntegerMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpIntegerMaxAggregateFree(tpl::sql::IntegerMaxAggregate *agg) {
  agg->~IntegerMaxAggregate();
}

VM_OP_HOT void OpRealMaxAggregateInit(tpl::sql::RealMaxAggregate *agg) {
  new (agg) tpl::sql::RealMaxAggregate();
}

VM_OP_HOT void OpRealMaxAggregateAdvance(tpl::sql::RealMaxAggregate *agg,
                                         const tpl::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMaxAggregateMerge(tpl::sql::RealMaxAggregate *agg_1,
                                       const tpl::sql::RealMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMaxAggregateReset(tpl::sql::RealMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealMaxAggregateGetResult(tpl::sql::Real *result,
                                           const tpl::sql::RealMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpRealMaxAggregateFree(tpl::sql::RealMaxAggregate *agg) { agg->~RealMaxAggregate(); }

VM_OP_HOT void OpDateMaxAggregateInit(tpl::sql::DateMaxAggregate *agg) {
  new (agg) tpl::sql::DateMaxAggregate();
}

VM_OP_HOT void OpDateMaxAggregateAdvance(tpl::sql::DateMaxAggregate *agg,
                                         const tpl::sql::DateVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpDateMaxAggregateMerge(tpl::sql::DateMaxAggregate *agg_1,
                                       const tpl::sql::DateMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpDateMaxAggregateReset(tpl::sql::DateMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpDateMaxAggregateGetResult(tpl::sql::DateVal *result,
                                           const tpl::sql::DateMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpDateMaxAggregateFree(tpl::sql::DateMaxAggregate *agg) { agg->~DateMaxAggregate(); }

VM_OP_HOT void OpStringMaxAggregateInit(tpl::sql::StringMaxAggregate *agg) {
  new (agg) tpl::sql::StringMaxAggregate();
}

VM_OP_HOT void OpStringMaxAggregateAdvance(tpl::sql::StringMaxAggregate *agg,
                                           const tpl::sql::StringVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpStringMaxAggregateMerge(tpl::sql::StringMaxAggregate *agg_1,
                                         const tpl::sql::StringMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpStringMaxAggregateReset(tpl::sql::StringMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpStringMaxAggregateGetResult(tpl::sql::StringVal *result,
                                             const tpl::sql::StringMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpStringMaxAggregateFree(tpl::sql::StringMaxAggregate *agg) {
  agg->~StringMaxAggregate();
}

// ---------------------------------------------------------
// MIN
// ---------------------------------------------------------

VM_OP_HOT void OpIntegerMinAggregateInit(tpl::sql::IntegerMinAggregate *agg) {
  new (agg) tpl::sql::IntegerMinAggregate();
}

VM_OP_HOT void OpIntegerMinAggregateAdvance(tpl::sql::IntegerMinAggregate *agg,
                                            const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMinAggregateMerge(tpl::sql::IntegerMinAggregate *agg_1,
                                          const tpl::sql::IntegerMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMinAggregateReset(tpl::sql::IntegerMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMinAggregateGetResult(tpl::sql::Integer *result,
                                              const tpl::sql::IntegerMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpIntegerMinAggregateFree(tpl::sql::IntegerMinAggregate *agg) {
  agg->~IntegerMinAggregate();
}

VM_OP_HOT void OpRealMinAggregateInit(tpl::sql::RealMinAggregate *agg) {
  new (agg) tpl::sql::RealMinAggregate();
}

VM_OP_HOT void OpRealMinAggregateAdvance(tpl::sql::RealMinAggregate *agg,
                                         const tpl::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMinAggregateMerge(tpl::sql::RealMinAggregate *agg_1,
                                       const tpl::sql::RealMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMinAggregateReset(tpl::sql::RealMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealMinAggregateGetResult(tpl::sql::Real *result,
                                           const tpl::sql::RealMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpRealMinAggregateFree(tpl::sql::RealMinAggregate *agg) { agg->~RealMinAggregate(); }

VM_OP_HOT void OpDateMinAggregateInit(tpl::sql::DateMinAggregate *agg) {
  new (agg) tpl::sql::DateMinAggregate();
}

VM_OP_HOT void OpDateMinAggregateAdvance(tpl::sql::DateMinAggregate *agg,
                                         const tpl::sql::DateVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpDateMinAggregateMerge(tpl::sql::DateMinAggregate *agg_1,
                                       const tpl::sql::DateMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpDateMinAggregateReset(tpl::sql::DateMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpDateMinAggregateGetResult(tpl::sql::DateVal *result,
                                           const tpl::sql::DateMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpDateMinAggregateFree(tpl::sql::DateMinAggregate *agg) { agg->~DateMinAggregate(); }

VM_OP_HOT void OpStringMinAggregateInit(tpl::sql::StringMinAggregate *agg) {
  new (agg) tpl::sql::StringMinAggregate();
}

VM_OP_HOT void OpStringMinAggregateAdvance(tpl::sql::StringMinAggregate *agg,
                                           const tpl::sql::StringVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpStringMinAggregateMerge(tpl::sql::StringMinAggregate *agg_1,
                                         const tpl::sql::StringMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpStringMinAggregateReset(tpl::sql::StringMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpStringMinAggregateGetResult(tpl::sql::StringVal *result,
                                             const tpl::sql::StringMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpStringMinAggregateFree(tpl::sql::StringMinAggregate *agg) {
  agg->~StringMinAggregate();
}

// ---------------------------------------------------------
// AVG
// ---------------------------------------------------------

VM_OP_HOT void OpAvgAggregateInit(tpl::sql::AvgAggregate *agg) {
  new (agg) tpl::sql::AvgAggregate();
}

VM_OP_HOT void OpAvgAggregateAdvanceInteger(tpl::sql::AvgAggregate *agg,
                                            const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpAvgAggregateAdvanceReal(tpl::sql::AvgAggregate *agg, const tpl::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpAvgAggregateMerge(tpl::sql::AvgAggregate *agg_1,
                                   const tpl::sql::AvgAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpAvgAggregateReset(tpl::sql::AvgAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpAvgAggregateGetResult(tpl::sql::Real *result, const tpl::sql::AvgAggregate *agg) {
  *result = agg->GetResultAvg();
}

VM_OP_HOT void OpAvgAggregateFree(tpl::sql::AvgAggregate *agg) { agg->~AvgAggregate(); }

// ---------------------------------------------------------
// Hash Joins
// ---------------------------------------------------------

VM_OP void OpJoinHashTableInit(tpl::sql::JoinHashTable *join_hash_table,
                               tpl::sql::MemoryPool *memory, uint32_t tuple_size);

VM_OP_HOT void OpJoinHashTableAllocTuple(byte **result, tpl::sql::JoinHashTable *join_hash_table,
                                         hash_t hash) {
  *result = join_hash_table->AllocInputTuple(hash);
}

VM_OP void OpJoinHashTableBuild(tpl::sql::JoinHashTable *join_hash_table);

VM_OP void OpJoinHashTableBuildParallel(tpl::sql::JoinHashTable *join_hash_table,
                                        tpl::sql::ThreadStateContainer *thread_state_container,
                                        uint32_t jht_offset);

VM_OP_HOT void OpJoinHashTableLookup(tpl::sql::JoinHashTable *join_hash_table,
                                     tpl::sql::HashTableEntryIterator *ht_entry_iter,
                                     const hash_t hash_val) {
  *ht_entry_iter = join_hash_table->Lookup<false>(hash_val);
}

VM_OP void OpJoinHashTableFree(tpl::sql::JoinHashTable *join_hash_table);

VM_OP void OpJoinHashTableVectorProbeInit(tpl::sql::JoinHashTableVectorProbe *jht_vector_probe,
                                          tpl::sql::JoinHashTable *jht);

VM_OP_HOT void OpJoinHashTableVectorProbePrepare(
    tpl::sql::JoinHashTableVectorProbe *jht_vector_probe, tpl::sql::VectorProjectionIterator *vpi,
    tpl::sql::JoinHashTableVectorProbe::HashFn hash_fn) {
  jht_vector_probe->Prepare(vpi, hash_fn);
}

VM_OP_HOT void OpJoinHashTableVectorProbeGetNextOutput(
    const byte **result, tpl::sql::JoinHashTableVectorProbe *jht_vector_probe,
    tpl::sql::VectorProjectionIterator *vpi,
    tpl::sql::JoinHashTableVectorProbe::KeyEqFn key_eq_fn) {
  *result = jht_vector_probe->GetNextOutput(vpi, key_eq_fn);
}

VM_OP void OpJoinHashTableVectorProbeFree(tpl::sql::JoinHashTableVectorProbe *jht_vector_probe);

VM_OP_HOT void OpHashTableEntryIteratorHasNext(bool *has_next,
                                               tpl::sql::HashTableEntryIterator *ht_entry_iter) {
  *has_next = ht_entry_iter->HasNext();
}

VM_OP_HOT void OpHashTableEntryIteratorGetRow(const byte **row,
                                              tpl::sql::HashTableEntryIterator *ht_entry_iter) {
  *row = ht_entry_iter->GetMatchPayload();
}

// ---------------------------------------------------------
// Sorting
// ---------------------------------------------------------

VM_OP void OpSorterInit(tpl::sql::Sorter *sorter, tpl::sql::MemoryPool *memory,
                        tpl::sql::Sorter::ComparisonFunction cmp_fn, uint32_t tuple_size);

VM_OP_HOT void OpSorterAllocTuple(byte **result, tpl::sql::Sorter *sorter) {
  *result = sorter->AllocInputTuple();
}

VM_OP_HOT void OpSorterAllocTupleTopK(byte **result, tpl::sql::Sorter *sorter, uint64_t top_k) {
  *result = sorter->AllocInputTupleTopK(top_k);
}

VM_OP_HOT void OpSorterAllocTupleTopKFinish(tpl::sql::Sorter *sorter, uint64_t top_k) {
  sorter->AllocInputTupleTopKFinish(top_k);
}

VM_OP void OpSorterSort(tpl::sql::Sorter *sorter);

VM_OP void OpSorterSortParallel(tpl::sql::Sorter *sorter,
                                tpl::sql::ThreadStateContainer *thread_state_container,
                                uint32_t sorter_offset);

VM_OP void OpSorterSortTopKParallel(tpl::sql::Sorter *sorter,
                                    tpl::sql::ThreadStateContainer *thread_state_container,
                                    uint32_t sorter_offset, uint64_t top_k);

VM_OP void OpSorterFree(tpl::sql::Sorter *sorter);

VM_OP void OpSorterIteratorInit(tpl::sql::SorterIterator *iter, tpl::sql::Sorter *sorter);

VM_OP_HOT void OpSorterIteratorHasNext(bool *has_more, tpl::sql::SorterIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpSorterIteratorNext(tpl::sql::SorterIterator *iter) { iter->Next(); }

VM_OP_HOT void OpSorterIteratorGetRow(const byte **row, tpl::sql::SorterIterator *iter) {
  *row = iter->GetRow();
}

VM_OP void OpSorterIteratorFree(tpl::sql::SorterIterator *iter);

// ---------------------------------------------------------
// Output
// ---------------------------------------------------------

VM_OP_WARM void OpResultBufferAllocOutputRow(byte **result, tpl::sql::ExecutionContext *ctx) {
  *result = ctx->GetResultBuffer()->AllocOutputSlot();
}

VM_OP_WARM void OpResultBufferFinalize(tpl::sql::ExecutionContext *ctx) {
  ctx->GetResultBuffer()->Finalize();
}

// ---------------------------------------------------------
// Trig functions
// ---------------------------------------------------------

VM_OP_WARM void OpPi(tpl::sql::Real *result) { tpl::sql::ArithmeticFunctions::Pi(result); }

VM_OP_WARM void OpE(tpl::sql::Real *result) { tpl::sql::ArithmeticFunctions::E(result); }

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

VM_OP_WARM void OpValIsNull(bool *result, const tpl::sql::Val *val) {
  *result = tpl::sql::IsNullPredicate::IsNull(*val);
}

VM_OP_WARM void OpValIsNotNull(bool *result, const tpl::sql::Val *val) {
  *result = tpl::sql::IsNullPredicate::IsNotNull(*val);
}

// ---------------------------------------------------------
// String functions
// ---------------------------------------------------------

VM_OP_WARM void OpCharLength(tpl::sql::ExecutionContext *ctx, tpl::sql::Integer *result,
                             const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::CharLength(ctx, result, *str);
}

VM_OP_WARM void OpLeft(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                       const tpl::sql::StringVal *str, const tpl::sql::Integer *n) {
  tpl::sql::StringFunctions::Left(ctx, result, *str, *n);
}

VM_OP_WARM void OpLike(tpl::sql::BoolVal *result, const tpl::sql::StringVal *str,
                       const tpl::sql::StringVal *pattern) {
  tpl::sql::StringFunctions::Like(nullptr, result, *str, *pattern);
}

VM_OP_WARM void OpLength(tpl::sql::ExecutionContext *ctx, tpl::sql::Integer *result,
                         const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Length(ctx, result, *str);
}

VM_OP_WARM void OpLower(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                        const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Lower(ctx, result, *str);
}

VM_OP_WARM void OpLPad(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                       const tpl::sql::StringVal *str, const tpl::sql::Integer *len,
                       const tpl::sql::StringVal *pad) {
  tpl::sql::StringFunctions::Lpad(ctx, result, *str, *len, *pad);
}

VM_OP_WARM void OpLTrim(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                        const tpl::sql::StringVal *str, const tpl::sql::StringVal *chars) {
  tpl::sql::StringFunctions::Ltrim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpRepeat(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                         const tpl::sql::StringVal *str, const tpl::sql::Integer *n) {
  tpl::sql::StringFunctions::Repeat(ctx, result, *str, *n);
}

VM_OP_WARM void OpReverse(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                          const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Reverse(ctx, result, *str);
}

VM_OP_WARM void OpRight(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                        const tpl::sql::StringVal *str, const tpl::sql::Integer *n) {
  tpl::sql::StringFunctions::Right(ctx, result, *str, *n);
}

VM_OP_WARM void OpRPad(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                       const tpl::sql::StringVal *str, const tpl::sql::Integer *n,
                       const tpl::sql::StringVal *pad) {
  tpl::sql::StringFunctions::Rpad(ctx, result, *str, *n, *pad);
}

VM_OP_WARM void OpRTrim(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                        const tpl::sql::StringVal *str, const tpl::sql::StringVal *chars) {
  tpl::sql::StringFunctions::Rtrim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpSplitPart(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                            const tpl::sql::StringVal *str, const tpl::sql::StringVal *delim,
                            const tpl::sql::Integer *field) {
  tpl::sql::StringFunctions::SplitPart(ctx, result, *str, *delim, *field);
}

VM_OP_WARM void OpSubstring(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                            const tpl::sql::StringVal *str, const tpl::sql::Integer *pos,
                            const tpl::sql::Integer *len) {
  tpl::sql::StringFunctions::Substring(ctx, result, *str, *pos, *len);
}

VM_OP_WARM void OpTrim(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                       const tpl::sql::StringVal *str, const tpl::sql::StringVal *chars) {
  tpl::sql::StringFunctions::Trim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpUpper(tpl::sql::ExecutionContext *ctx, tpl::sql::StringVal *result,
                        const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Upper(ctx, result, *str);
}

// Macro hygiene
#undef VM_OP_COLD
#undef VM_OP_WARM
#undef VM_OP_HOT
#undef VM_OP

}  // extern "C"
