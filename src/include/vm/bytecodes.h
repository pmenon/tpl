#pragma once

#include <algorithm>
#include <cstdint>

#include "common/common.h"
#include "common/macros.h"
#include "vm/bytecode_operands.h"

namespace tpl::vm {

// Creates instances of a given opcode for all integer primitive types
#define CREATE_FOR_INT_TYPES(F, op, ...) \
  F(op##_##int8_t, __VA_ARGS__)          \
  F(op##_##int16_t, __VA_ARGS__)         \
  F(op##_##int32_t, __VA_ARGS__)         \
  F(op##_##int64_t, __VA_ARGS__)         \
  F(op##_##uint8_t, __VA_ARGS__)         \
  F(op##_##uint16_t, __VA_ARGS__)        \
  F(op##_##uint32_t, __VA_ARGS__)        \
  F(op##_##uint64_t, __VA_ARGS__)

// Creates instances of a given opcode for all floating-point primitive types
#define CREATE_FOR_FLOAT_TYPES(func, op) func(op, float) func(op, double)

// Creates instances of a given opcode for *ALL* primitive types
#define CREATE_FOR_ALL_TYPES(F, op, ...)   \
  CREATE_FOR_INT_TYPES(F, op, __VA_ARGS__) \
  CREATE_FOR_FLOAT_TYPES(F, op, __VA_ARGS__)

#define GET_BASE_FOR_INT_TYPES(op) (op##_int8_t)
#define GET_BASE_FOR_FLOAT_TYPES(op) (op##_float)
#define GET_BASE_FOR_BOOL_TYPES(op) (op##_bool)

/**
 * The master list of all bytecodes, flags and operands
 */
// clang-format off
#define BYTECODE_LIST(F)                                                                                               \
  /* Primitive operations */                                                                                           \
  CREATE_FOR_INT_TYPES(F, Add, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  CREATE_FOR_INT_TYPES(F, Sub, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  CREATE_FOR_INT_TYPES(F, Mul, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  CREATE_FOR_INT_TYPES(F, Div, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  CREATE_FOR_INT_TYPES(F, Rem, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  CREATE_FOR_INT_TYPES(F, BitAnd, OperandType::Local, OperandType::Local, OperandType::Local)                          \
  CREATE_FOR_INT_TYPES(F, BitOr, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  CREATE_FOR_INT_TYPES(F, BitXor, OperandType::Local, OperandType::Local, OperandType::Local)                          \
  CREATE_FOR_INT_TYPES(F, Neg, OperandType::Local, OperandType::Local)                                                 \
  CREATE_FOR_INT_TYPES(F, BitNeg, OperandType::Local, OperandType::Local)                                              \
  CREATE_FOR_INT_TYPES(F, GreaterThan, OperandType::Local, OperandType::Local, OperandType::Local)                     \
  CREATE_FOR_INT_TYPES(F, GreaterThanEqual, OperandType::Local, OperandType::Local, OperandType::Local)                \
  CREATE_FOR_INT_TYPES(F, Equal, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  CREATE_FOR_INT_TYPES(F, LessThan, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  CREATE_FOR_INT_TYPES(F, LessThanEqual, OperandType::Local, OperandType::Local, OperandType::Local)                   \
  CREATE_FOR_INT_TYPES(F, NotEqual, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  /* Boolean compliment */                                                                                             \
  F(Not, OperandType::Local, OperandType::Local)                                                                       \
                                                                                                                       \
  /* Branching */                                                                                                      \
  F(Jump, OperandType::JumpOffset)                                                                                     \
  F(JumpIfTrue, OperandType::Local, OperandType::JumpOffset)                                                           \
  F(JumpIfFalse, OperandType::Local, OperandType::JumpOffset)                                                          \
                                                                                                                       \
  /* Memory/pointer operations */                                                                                      \
  F(IsNullPtr, OperandType::Local, OperandType::Local)                                                                 \
  F(IsNotNullPtr, OperandType::Local, OperandType::Local)                                                              \
  F(Deref1, OperandType::Local, OperandType::Local)                                                                    \
  F(Deref2, OperandType::Local, OperandType::Local)                                                                    \
  F(Deref4, OperandType::Local, OperandType::Local)                                                                    \
  F(Deref8, OperandType::Local, OperandType::Local)                                                                    \
  F(DerefN, OperandType::Local, OperandType::Local, OperandType::UImm4)                                                \
  F(Assign1, OperandType::Local, OperandType::Local)                                                                   \
  F(Assign2, OperandType::Local, OperandType::Local)                                                                   \
  F(Assign4, OperandType::Local, OperandType::Local)                                                                   \
  F(Assign8, OperandType::Local, OperandType::Local)                                                                   \
  F(AssignImm1, OperandType::Local, OperandType::Imm1)                                                                 \
  F(AssignImm2, OperandType::Local, OperandType::Imm2)                                                                 \
  F(AssignImm4, OperandType::Local, OperandType::Imm4)                                                                 \
  F(AssignImm8, OperandType::Local, OperandType::Imm8)                                                                 \
  F(AssignImm4F, OperandType::Local, OperandType::Imm4F)                                                               \
  F(AssignImm8F, OperandType::Local, OperandType::Imm8F)                                                               \
  F(Lea, OperandType::Local, OperandType::Local, OperandType::Imm4)                                                    \
  F(LeaScaled, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Imm4, OperandType::Imm4)       \
                                                                                                                       \
  /* Function calls */                                                                                                 \
  F(Call, OperandType::FunctionId, OperandType::LocalCount)                                                            \
  F(Return)                                                                                                            \
                                                                                                                       \
  /* Execution Context */                                                                                              \
  F(ExecutionContextGetMemoryPool, OperandType::Local, OperandType::Local)                                             \
                                                                                                                       \
  /* Thread State Container */                                                                                         \
  F(ThreadStateContainerInit, OperandType::Local, OperandType::Local)                                                  \
  F(ThreadStateContainerIterate, OperandType::Local, OperandType::Local, OperandType::FunctionId)                      \
  F(ThreadStateContainerReset, OperandType::Local, OperandType::Local, OperandType::FunctionId,                        \
      OperandType::FunctionId, OperandType::Local)                                                                     \
  F(ThreadStateContainerFree, OperandType::Local)                                                                      \
                                                                                                                       \
  /* Table Vector Iterator */                                                                                          \
  F(TableVectorIteratorInit, OperandType::Local, OperandType::UImm2)                                                   \
  F(TableVectorIteratorPerformInit, OperandType::Local)                                                                \
  F(TableVectorIteratorNext, OperandType::Local, OperandType::Local)                                                   \
  F(TableVectorIteratorFree, OperandType::Local)                                                                       \
  F(TableVectorIteratorGetVPI, OperandType::Local, OperandType::Local)                                                 \
  F(ParallelScanTable, OperandType::UImm2, OperandType::Local, OperandType::Local, OperandType::FunctionId)            \
                                                                                                                       \
  /* Vector Projection Iterator (VPI) */                                                                               \
  F(VPIIsFiltered, OperandType::Local, OperandType::Local)                                                             \
  F(VPIGetSelectedRowCount, OperandType::Local, OperandType::Local)                                                    \
  F(VPIHasNext, OperandType::Local, OperandType::Local)                                                                \
  F(VPIHasNextFiltered, OperandType::Local, OperandType::Local)                                                        \
  F(VPIAdvance, OperandType::Local)                                                                                    \
  F(VPIAdvanceFiltered, OperandType::Local)                                                                            \
  F(VPISetPosition, OperandType::Local, OperandType::Local)                                                            \
  F(VPISetPositionFiltered, OperandType::Local, OperandType::Local)                                                    \
  F(VPIMatch, OperandType::Local, OperandType::Local)                                                                  \
  F(VPIReset, OperandType::Local)                                                                                      \
  F(VPIResetFiltered, OperandType::Local)                                                                              \
  F(VPIGetSmallInt, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPIGetInteger, OperandType::Local, OperandType::Local, OperandType::UImm4)                                         \
  F(VPIGetBigInt, OperandType::Local, OperandType::Local, OperandType::UImm4)                                          \
  F(VPIGetReal, OperandType::Local, OperandType::Local, OperandType::UImm4)                                            \
  F(VPIGetDouble, OperandType::Local, OperandType::Local, OperandType::UImm4)                                          \
  F(VPIGetDecimal, OperandType::Local, OperandType::Local, OperandType::UImm4)                                         \
  F(VPIGetDate, OperandType::Local, OperandType::Local, OperandType::UImm4)                                            \
  F(VPIGetString, OperandType::Local, OperandType::Local, OperandType::UImm4)                                          \
  F(VPIGetSmallIntNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                    \
  F(VPIGetIntegerNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                     \
  F(VPIGetBigIntNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                      \
  F(VPIGetRealNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPIGetDoubleNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                      \
  F(VPIGetDecimalNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                     \
  F(VPIGetDateNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPIGetStringNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                      \
  F(VPISetSmallInt, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPISetInteger, OperandType::Local, OperandType::Local, OperandType::UImm4)                                         \
  F(VPISetBigInt, OperandType::Local, OperandType::Local, OperandType::UImm4)                                          \
  F(VPISetReal, OperandType::Local, OperandType::Local, OperandType::UImm4)                                            \
  F(VPISetDouble, OperandType::Local, OperandType::Local, OperandType::UImm4)                                          \
  F(VPISetDecimal, OperandType::Local, OperandType::Local, OperandType::UImm4)                                         \
  F(VPISetDate, OperandType::Local, OperandType::Local, OperandType::UImm4)                                            \
  F(VPISetString, OperandType::Local, OperandType::Local, OperandType::UImm4)                                          \
  F(VPISetSmallIntNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                    \
  F(VPISetIntegerNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                     \
  F(VPISetBigIntNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                      \
  F(VPISetRealNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPISetDoubleNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                      \
  F(VPISetDecimalNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                     \
  F(VPISetDateNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPISetStringNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                      \
                                                                                                                       \
  /* Filter Manager */                                                                                                 \
  F(FilterManagerInit, OperandType::Local)                                                                             \
  F(FilterManagerStartNewClause, OperandType::Local)                                                                   \
  F(FilterManagerInsertFilter, OperandType::Local, OperandType::FunctionId)                                            \
  F(FilterManagerFinalize, OperandType::Local)                                                                         \
  F(FilterManagerRunFilters, OperandType::Local, OperandType::Local)                                                   \
  F(FilterManagerFree, OperandType::Local)                                                                             \
                                                                                                                       \
  /* Vector Filter Executor */                                                                                         \
  F(VectorFilterEqual, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                 \
  F(VectorFilterEqualVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)              \
  F(VectorFilterGreaterThan, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)           \
  F(VectorFilterGreaterThanVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)        \
  F(VectorFilterGreaterThanEqual, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)      \
  F(VectorFilterGreaterThanEqualVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)   \
  F(VectorFilterLessThan, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)              \
  F(VectorFilterLessThanVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)           \
  F(VectorFilterLessThanEqual, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)         \
  F(VectorFilterLessThanEqualVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)      \
  F(VectorFilterNotEqual, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)              \
  F(VectorFilterNotEqualVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)           \
                                                                                                                       \
  /* SQL type comparisons */                                                                                           \
  F(ForceBoolTruth, OperandType::Local, OperandType::Local)                                                            \
  F(InitBool, OperandType::Local, OperandType::Local)                                                                  \
  F(InitInteger, OperandType::Local, OperandType::Local)                                                               \
  F(InitReal, OperandType::Local, OperandType::Local)                                                                  \
  F(InitDate, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                          \
  F(InitString, OperandType::Local, OperandType::StaticLocal, OperandType::UImm4)                                      \
  F(IntegerToReal, OperandType::Local, OperandType::Local)                                                             \
  F(RealToInteger, OperandType::Local, OperandType::Local)                                                             \
  F(LessThanInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(LessThanEqualInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                  \
  F(GreaterThanInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(GreaterThanEqualInteger, OperandType::Local, OperandType::Local, OperandType::Local)                               \
  F(EqualInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                          \
  F(NotEqualInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(LessThanReal, OperandType::Local, OperandType::Local, OperandType::Local)                                          \
  F(LessThanEqualReal, OperandType::Local, OperandType::Local, OperandType::Local)                                     \
  F(GreaterThanReal, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(GreaterThanEqualReal, OperandType::Local, OperandType::Local, OperandType::Local)                                  \
  F(EqualReal, OperandType::Local, OperandType::Local, OperandType::Local)                                             \
  F(NotEqualReal, OperandType::Local, OperandType::Local, OperandType::Local)                                          \
  F(LessThanString, OperandType::Local, OperandType::Local, OperandType::Local)                                        \
  F(LessThanEqualString, OperandType::Local, OperandType::Local, OperandType::Local)                                   \
  F(GreaterThanString, OperandType::Local, OperandType::Local, OperandType::Local)                                     \
  F(GreaterThanEqualString, OperandType::Local, OperandType::Local, OperandType::Local)                                \
  F(EqualString, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(NotEqualString, OperandType::Local, OperandType::Local, OperandType::Local)                                        \
  F(LessThanDate, OperandType::Local, OperandType::Local, OperandType::Local)                                          \
  F(LessThanEqualDate, OperandType::Local, OperandType::Local, OperandType::Local)                                     \
  F(GreaterThanDate, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(GreaterThanEqualDate, OperandType::Local, OperandType::Local, OperandType::Local)                                  \
  F(EqualDate, OperandType::Local, OperandType::Local, OperandType::Local)                                             \
  F(NotEqualDate, OperandType::Local, OperandType::Local, OperandType::Local)                                          \
                                                                                                                       \
  /* SQL value unary operations */                                                                                     \
  F(AbsInteger, OperandType::Local, OperandType::Local)                                                                \
  F(AbsReal, OperandType::Local, OperandType::Local)                                                                   \
  F(ValIsNull, OperandType::Local, OperandType::Local)                                                                 \
  F(ValIsNotNull, OperandType::Local, OperandType::Local)                                                              \
  /* SQL value binary operations */                                                                                    \
  F(AddInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(SubInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(MulInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(DivInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(RemInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(AddReal, OperandType::Local, OperandType::Local, OperandType::Local)                                               \
  F(SubReal, OperandType::Local, OperandType::Local, OperandType::Local)                                               \
  F(MulReal, OperandType::Local, OperandType::Local, OperandType::Local)                                               \
  F(DivReal, OperandType::Local, OperandType::Local, OperandType::Local)                                               \
  F(RemReal, OperandType::Local, OperandType::Local, OperandType::Local)                                               \
                                                                                                                       \
  /* Hashing */                                                                                                        \
  F(HashInt, OperandType::Local, OperandType::Local, OperandType::Local)                                               \
  F(HashReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(HashString, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(HashDate, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(HashCombine, OperandType::Local, OperandType::Local)                                                               \
                                                                                                                       \
  /* Aggregation Hash Table */                                                                                         \
  F(AggregationHashTableInit, OperandType::Local, OperandType::Local, OperandType::Local)                              \
  F(AggregationHashTableAllocTuple, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  F(AggregationHashTableAllocTuplePartitioned, OperandType::Local, OperandType::Local, OperandType::Local)             \
  F(AggregationHashTableLinkHashTableEntry, OperandType::Local, OperandType::Local)                                    \
  F(AggregationHashTableLookup, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::FunctionId,   \
      OperandType::Local)                                                                                              \
  F(AggregationHashTableProcessBatch, OperandType::Local, OperandType::Local, OperandType::FunctionId,                 \
      OperandType::FunctionId, OperandType::FunctionId, OperandType::FunctionId, OperandType::Local)                   \
  F(AggregationHashTableTransferPartitions, OperandType::Local, OperandType::Local, OperandType::Local,                \
      OperandType::FunctionId)                                                                                         \
  F(AggregationHashTableParallelPartitionedScan, OperandType::Local, OperandType::Local, OperandType::Local,           \
      OperandType::FunctionId)                                                                                         \
  F(AggregationHashTableFree, OperandType::Local)                                                                      \
  F(AggregationHashTableIteratorInit, OperandType::Local, OperandType::Local)                                          \
  F(AggregationHashTableIteratorHasNext, OperandType::Local, OperandType::Local)                                       \
  F(AggregationHashTableIteratorNext, OperandType::Local)                                                              \
  F(AggregationHashTableIteratorGetRow, OperandType::Local, OperandType::Local)                                        \
  F(AggregationHashTableIteratorFree, OperandType::Local)                                                              \
  F(AggregationOverflowPartitionIteratorHasNext, OperandType::Local, OperandType::Local)                               \
  F(AggregationOverflowPartitionIteratorNext, OperandType::Local)                                                      \
  F(AggregationOverflowPartitionIteratorGetHash, OperandType::Local, OperandType::Local)                               \
  F(AggregationOverflowPartitionIteratorGetRow, OperandType::Local, OperandType::Local)                                \
  F(AggregationOverflowPartitionIteratorGetRowEntry, OperandType::Local, OperandType::Local)                           \
  /* Aggregates */                                                                                                     \
  F(CountAggregateInit, OperandType::Local)                                                                            \
  F(CountAggregateAdvance, OperandType::Local, OperandType::Local)                                                     \
  F(CountAggregateMerge, OperandType::Local, OperandType::Local)                                                       \
  F(CountAggregateReset, OperandType::Local)                                                                           \
  F(CountAggregateGetResult, OperandType::Local, OperandType::Local)                                                   \
  F(CountAggregateFree, OperandType::Local)                                                                            \
  F(CountStarAggregateInit, OperandType::Local)                                                                        \
  F(CountStarAggregateAdvance, OperandType::Local, OperandType::Local)                                                 \
  F(CountStarAggregateMerge, OperandType::Local, OperandType::Local)                                                   \
  F(CountStarAggregateReset, OperandType::Local)                                                                       \
  F(CountStarAggregateGetResult, OperandType::Local, OperandType::Local)                                               \
  F(CountStarAggregateFree, OperandType::Local)                                                                        \
  F(IntegerSumAggregateInit, OperandType::Local)                                                                       \
  F(IntegerSumAggregateAdvance, OperandType::Local, OperandType::Local)                                                \
  F(IntegerSumAggregateMerge, OperandType::Local, OperandType::Local)                                                  \
  F(IntegerSumAggregateReset, OperandType::Local)                                                                      \
  F(IntegerSumAggregateGetResult, OperandType::Local, OperandType::Local)                                              \
  F(IntegerSumAggregateFree, OperandType::Local)                                                                       \
  F(IntegerMaxAggregateInit, OperandType::Local)                                                                       \
  F(IntegerMaxAggregateAdvance, OperandType::Local, OperandType::Local)                                                \
  F(IntegerMaxAggregateMerge, OperandType::Local, OperandType::Local)                                                  \
  F(IntegerMaxAggregateReset, OperandType::Local)                                                                      \
  F(IntegerMaxAggregateGetResult, OperandType::Local, OperandType::Local)                                              \
  F(IntegerMaxAggregateFree, OperandType::Local)                                                                       \
  F(IntegerMinAggregateInit, OperandType::Local)                                                                       \
  F(IntegerMinAggregateAdvance, OperandType::Local, OperandType::Local)                                                \
  F(IntegerMinAggregateMerge, OperandType::Local, OperandType::Local)                                                  \
  F(IntegerMinAggregateReset, OperandType::Local)                                                                      \
  F(IntegerMinAggregateGetResult, OperandType::Local, OperandType::Local)                                              \
  F(IntegerMinAggregateFree, OperandType::Local)                                                                       \
  F(AvgAggregateInit, OperandType::Local)                                                                              \
  F(AvgAggregateAdvanceInteger, OperandType::Local, OperandType::Local)                                                \
  F(AvgAggregateAdvanceReal, OperandType::Local, OperandType::Local)                                                   \
  F(AvgAggregateMerge, OperandType::Local, OperandType::Local)                                                         \
  F(AvgAggregateReset, OperandType::Local)                                                                             \
  F(AvgAggregateGetResult, OperandType::Local, OperandType::Local)                                                     \
  F(AvgAggregateFree, OperandType::Local)                                                                              \
  F(RealSumAggregateInit, OperandType::Local)                                                                          \
  F(RealSumAggregateAdvance, OperandType::Local, OperandType::Local)                                                   \
  F(RealSumAggregateMerge, OperandType::Local, OperandType::Local)                                                     \
  F(RealSumAggregateReset, OperandType::Local)                                                                         \
  F(RealSumAggregateGetResult, OperandType::Local, OperandType::Local)                                                 \
  F(RealSumAggregateFree, OperandType::Local)                                                                          \
  F(RealMaxAggregateInit, OperandType::Local)                                                                          \
  F(RealMaxAggregateAdvance, OperandType::Local, OperandType::Local)                                                   \
  F(RealMaxAggregateMerge, OperandType::Local, OperandType::Local)                                                     \
  F(RealMaxAggregateReset, OperandType::Local)                                                                         \
  F(RealMaxAggregateGetResult, OperandType::Local, OperandType::Local)                                                 \
  F(RealMaxAggregateFree, OperandType::Local)                                                                          \
  F(RealMinAggregateInit, OperandType::Local)                                                                          \
  F(RealMinAggregateAdvance, OperandType::Local, OperandType::Local)                                                   \
  F(RealMinAggregateMerge, OperandType::Local, OperandType::Local)                                                     \
  F(RealMinAggregateReset, OperandType::Local)                                                                         \
  F(RealMinAggregateGetResult, OperandType::Local, OperandType::Local)                                                 \
  F(RealMinAggregateFree, OperandType::Local)                                                                          \
                                                                                                                       \
  /* Hash Joins */                                                                                                     \
  F(JoinHashTableInit, OperandType::Local, OperandType::Local, OperandType::Local)                                     \
  F(JoinHashTableAllocTuple, OperandType::Local, OperandType::Local, OperandType::Local)                               \
  F(JoinHashTableBuild, OperandType::Local)                                                                            \
  F(JoinHashTableBuildParallel, OperandType::Local, OperandType::Local, OperandType::Local)                            \
  F(JoinHashTableLookup, OperandType::Local, OperandType::Local, OperandType::Local)                                   \
  F(JoinHashTableFree, OperandType::Local)                                                                             \
  F(JoinHashTableVectorProbeInit, OperandType::Local, OperandType::Local)                                              \
  F(JoinHashTableVectorProbePrepare, OperandType::Local, OperandType::Local, OperandType::FunctionId)                  \
  F(JoinHashTableVectorProbeGetNextOutput, OperandType::Local, OperandType::Local, OperandType::Local,                 \
      OperandType::FunctionId)                                                                                         \
  F(JoinHashTableVectorProbeFree, OperandType::Local)                                                                  \
  F(HashTableEntryIteratorHasNext, OperandType::Local, OperandType::Local, OperandType::FunctionId,                    \
      OperandType::Local, OperandType::Local)                                                                          \
  F(HashTableEntryIteratorGetRow, OperandType::Local, OperandType::Local)                                              \
                                                                                                                       \
  /* Sorting */                                                                                                        \
  F(SorterInit, OperandType::Local, OperandType::Local, OperandType::FunctionId, OperandType::Local)                   \
  F(SorterAllocTuple, OperandType::Local, OperandType::Local)                                                          \
  F(SorterAllocTupleTopK, OperandType::Local, OperandType::Local, OperandType::Local)                                  \
  F(SorterAllocTupleTopKFinish, OperandType::Local, OperandType::Local)                                                \
  F(SorterSort, OperandType::Local)                                                                                    \
  F(SorterSortParallel, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(SorterSortTopKParallel, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)            \
  F(SorterFree, OperandType::Local)                                                                                    \
  F(SorterIteratorInit, OperandType::Local, OperandType::Local)                                                        \
  F(SorterIteratorGetRow, OperandType::Local, OperandType::Local)                                                      \
  F(SorterIteratorHasNext, OperandType::Local, OperandType::Local)                                                     \
  F(SorterIteratorNext, OperandType::Local)                                                                            \
  F(SorterIteratorFree, OperandType::Local)                                                                            \
                                                                                                                       \
  /* Output */                                                                                                         \
  F(ResultBufferAllocOutputRow, OperandType::Local, OperandType::Local)                                                \
  F(ResultBufferFinalize, OperandType::Local)                                                                          \
                                                                                                                       \
  /* Trig functions */                                                                                                 \
  F(Pi, OperandType::Local)                                                                                            \
  F(E, OperandType::Local)                                                                                             \
  F(Acos, OperandType::Local, OperandType::Local)                                                                      \
  F(Asin, OperandType::Local, OperandType::Local)                                                                      \
  F(Atan, OperandType::Local, OperandType::Local)                                                                      \
  F(Atan2, OperandType::Local, OperandType::Local, OperandType::Local)                                                 \
  F(Cos, OperandType::Local, OperandType::Local)                                                                       \
  F(Cot, OperandType::Local, OperandType::Local)                                                                       \
  F(Sin, OperandType::Local, OperandType::Local)                                                                       \
  F(Tan, OperandType::Local, OperandType::Local)                                                                       \
  F(Cosh, OperandType::Local, OperandType::Local)                                                                      \
  F(Tanh, OperandType::Local, OperandType::Local)                                                                      \
  F(Sinh, OperandType::Local, OperandType::Local)                                                                      \
  F(Sqrt, OperandType::Local, OperandType::Local)                                                                      \
  F(Cbrt, OperandType::Local, OperandType::Local)                                                                      \
  F(Exp, OperandType::Local, OperandType::Local)                                                                       \
  F(Ceil, OperandType::Local, OperandType::Local)                                                                      \
  F(Floor, OperandType::Local, OperandType::Local)                                                                     \
  F(Truncate, OperandType::Local, OperandType::Local)                                                                  \
  F(Ln, OperandType::Local, OperandType::Local)                                                                        \
  F(Log2, OperandType::Local, OperandType::Local)                                                                      \
  F(Log10, OperandType::Local, OperandType::Local)                                                                     \
  F(Sign, OperandType::Local, OperandType::Local)                                                                      \
  F(Radians, OperandType::Local, OperandType::Local)                                                                   \
  F(Degrees, OperandType::Local, OperandType::Local)                                                                   \
  F(Round, OperandType::Local, OperandType::Local)                                                                     \
  F(RoundUpTo, OperandType::Local, OperandType::Local, OperandType::Local)                                             \
  F(Log, OperandType::Local, OperandType::Local, OperandType::Local)                                                   \
  F(Pow, OperandType::Local, OperandType::Local, OperandType::Local)                                                   \
                                                                                                                       \
  /* String functions */                                                                                               \
  F(Left, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                              \
  F(Length, OperandType::Local, OperandType::Local, OperandType::Local)                                                \
  F(Like, OperandType::Local, OperandType::Local, OperandType::Local)                                                  \
  F(Lower, OperandType::Local, OperandType::Local, OperandType::Local)                                                 \
  F(LPad, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)          \
  F(LTrim, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  F(Repeat, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                            \
  F(Reverse, OperandType::Local, OperandType::Local, OperandType::Local)                                               \
  F(Right, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  F(RPad, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)          \
  F(RTrim, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  F(SplitPart, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)     \
  F(Substring, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)     \
  F(Trim, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                              \
  F(Upper, OperandType::Local, OperandType::Local, OperandType::Local)

// clang-format on

/**
 * The enumeration listing all possible bytecode instructions.
 */
enum class Bytecode : uint32_t {
#define DECLARE_OP(inst, ...) inst,
  BYTECODE_LIST(DECLARE_OP)
#undef DECLARE_OP
#define COUNT_OP(inst, ...) +1
      Last = -1 BYTECODE_LIST(COUNT_OP)
#undef COUNT_OP
};

/**
 * Helper class for querying/interacting with bytecode instructions.
 */
class Bytecodes {
 public:
  // The total number of bytecode instructions
  static constexpr const uint32_t kBytecodeCount = static_cast<uint32_t>(Bytecode::Last) + 1;

  /**
   * @return The total number of bytecodes.
   */
  static constexpr uint32_t NumBytecodes() { return kBytecodeCount; }

  /**
   * @return The maximum length of any bytecode instruction in bytes.
   */
  static uint32_t MaxBytecodeNameLength();

  /**
   * @return The string representation of the given bytecode.
   */
  static const char *ToString(Bytecode bytecode) {
    return kBytecodeNames[static_cast<uint32_t>(bytecode)];
  }

  /**
   * @return The number of operands a bytecode accepts.
   */
  static uint32_t NumOperands(Bytecode bytecode) {
    return kBytecodeOperandCounts[static_cast<uint32_t>(bytecode)];
  }

  /**
   * @return An array of the operand types to the given bytecode.
   */
  static const OperandType *GetOperandTypes(Bytecode bytecode) {
    return kBytecodeOperandTypes[static_cast<uint32_t>(bytecode)];
  }

  /**
   * @return An array containing the sizes of all operands to the given bytecode.
   */
  static const OperandSize *GetOperandSizes(Bytecode bytecode) {
    return kBytecodeOperandSizes[static_cast<uint32_t>(bytecode)];
  }

  /**
   * @return The type of the Nth operand to the given bytecode.
   */
  static OperandType GetNthOperandType(Bytecode bytecode, uint32_t operand_index) {
    TPL_ASSERT(operand_index < NumOperands(bytecode),
               "Accessing out-of-bounds operand number for bytecode");
    return GetOperandTypes(bytecode)[operand_index];
  }

  /**
   * @return The size of the Nth operand to the given bytecode.
   */
  static OperandSize GetNthOperandSize(Bytecode bytecode, uint32_t operand_index) {
    TPL_ASSERT(operand_index < NumOperands(bytecode),
               "Accessing out-of-bounds operand number for bytecode");
    return GetOperandSizes(bytecode)[operand_index];
  }

  /**
   * @return The offset of the Nth operand of the given bytecode.
   */
  static uint32_t GetNthOperandOffset(Bytecode bytecode, uint32_t operand_index);

  /**
   * @return The name of the bytecode handler function for the given bytecode.
   */
  static const char *GetBytecodeHandlerName(Bytecode bytecode) {
    return kBytecodeHandlerName[ToByte(bytecode)];
  }

  /**
   * Converts the bytecode instruction @em bytecode into a raw encoded value.
   * @param bytecode The bytecode to convert.
   * @return The raw encoded value for the input bytecode instruction.
   */
  static constexpr std::underlying_type_t<Bytecode> ToByte(Bytecode bytecode) {
    TPL_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return static_cast<std::underlying_type_t<Bytecode>>(bytecode);
  }

  /**
   * Decode and convert the raw value @em val into a bytecode instruction.
   * @param val The value to convert.
   * @return The bytecode associated with the given value.
   */
  static constexpr Bytecode FromByte(std::underlying_type_t<Bytecode> val) {
    auto bytecode = static_cast<Bytecode>(val);
    TPL_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return bytecode;
  }

  /**
   * @return True if the bytecode @em bytecode is an unconditional jump; false otherwise.
   */
  static constexpr bool IsUnconditionalJump(Bytecode bytecode) {
    return bytecode == Bytecode::Jump;
  }

  /**
   * @return True if the bytecode @em bytecode is a conditional jump; false otherwise.
   */
  static constexpr bool IsConditionalJump(Bytecode bytecode) {
    return bytecode == Bytecode::JumpIfFalse || bytecode == Bytecode::JumpIfTrue;
  }

  /**
   * @return True if the bytecode @em bytecode is a jump instruction, either conditional or not;
   *         false otherwise.
   */
  static constexpr bool IsJump(Bytecode bytecode) {
    return IsConditionalJump(bytecode) || IsUnconditionalJump(bytecode);
  }

  /**
   * @return True if the bytecode @em bytecode is a return instruction.
   */
  static constexpr bool IsReturn(Bytecode bytecode) { return bytecode == Bytecode::Return; }

  /**
   * @return True if the bytecode @em bytecode is a terminal instruction. A terminal instruction is
   *         one that appears at the end of a basic block.
   */
  static constexpr bool IsTerminal(Bytecode bytecode) {
    return IsJump(bytecode) || IsReturn(bytecode);
  }

 private:
  static const char *kBytecodeNames[];
  static uint32_t kBytecodeOperandCounts[];
  static const OperandType *kBytecodeOperandTypes[];
  static const OperandSize *kBytecodeOperandSizes[];
  static const char *kBytecodeHandlerName[];
};

}  // namespace tpl::vm
