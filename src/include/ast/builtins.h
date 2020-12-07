#pragma once

#include "common/common.h"

namespace tpl::ast {

// The list of all builtin functions
// Args: internal name, function name
#define BUILTINS_LIST(F)                                        \
  /* Primitive <-> SQL */                                       \
  F(IntToSql, intToSql)                                         \
  F(BoolToSql, boolToSql)                                       \
  F(FloatToSql, floatToSql)                                     \
  F(DateToSql, dateToSql)                                       \
  F(StringToSql, stringToSql)                                   \
  F(SqlToBool, sqlToBool)                                       \
  F(IsValNull, isValNull)                                       \
  F(InitSqlNull, initSqlNull)                                   \
                                                                \
  /* SQL Conversions */                                         \
  F(ConvertBoolToInteger, convertBoolToInt)                     \
  F(ConvertIntegerToReal, convertIntToReal)                     \
  F(ConvertDateToTimestamp, convertDateToTime)                  \
  F(ConvertStringToBool, convertStringToBool)                   \
  F(ConvertStringToInt, convertStringToInt)                     \
  F(ConvertStringToReal, convertStringToReal)                   \
  F(ConvertStringToDate, convertStringToDate)                   \
  F(ConvertStringToTime, convertStringToTime)                   \
                                                                \
  /* SQL Functions */                                           \
  F(Like, like)                                                 \
  F(ExtractYear, extractYear)                                   \
  F(Concat, concat)                                             \
                                                                \
  /* Thread State Container */                                  \
  F(ExecutionContextGetMemoryPool, execCtxGetMem)               \
  F(ExecutionContextGetTLS, execCtxGetTLS)                      \
  F(ThreadStateContainerReset, tlsReset)                        \
  F(ThreadStateContainerGetState, tlsGetCurrentThreadState)     \
  F(ThreadStateContainerIterate, tlsIterate)                    \
  F(ThreadStateContainerClear, tlsClear)                        \
                                                                \
  /* Table scans */                                             \
  F(TableIterInit, tableIterInit)                               \
  F(TableIterAdvance, tableIterAdvance)                         \
  F(TableIterGetVPI, tableIterGetVPI)                           \
  F(TableIterClose, tableIterClose)                             \
  F(TableIterParallel, iterateTableParallel)                    \
                                                                \
  /* VPI */                                                     \
  F(VPIInit, vpiInit)                                           \
  F(VPIIsFiltered, vpiIsFiltered)                               \
  F(VPIGetSelectedRowCount, vpiSelectedRowCount)                \
  F(VPIGetVectorProjection, vpiGetVectorProjection)             \
  F(VPIHasNext, vpiHasNext)                                     \
  F(VPIAdvance, vpiAdvance)                                     \
  F(VPISetPosition, vpiSetPosition)                             \
  F(VPIMatch, vpiMatch)                                         \
  F(VPIReset, vpiReset)                                         \
  F(VPIGetBool, vpiGetBool)                                     \
  F(VPIGetTinyInt, vpiGetTinyInt)                               \
  F(VPIGetSmallInt, vpiGetSmallInt)                             \
  F(VPIGetInt, vpiGetInt)                                       \
  F(VPIGetBigInt, vpiGetBigInt)                                 \
  F(VPIGetReal, vpiGetReal)                                     \
  F(VPIGetDouble, vpiGetDouble)                                 \
  F(VPIGetDate, vpiGetDate)                                     \
  F(VPIGetString, vpiGetString)                                 \
  F(VPIGetPointer, vpiGetPointer)                               \
  F(VPISetBool, vpiSetBool)                                     \
  F(VPISetTinyInt, vpiSetTinyInt)                               \
  F(VPISetSmallInt, vpiSetSmallInt)                             \
  F(VPISetInt, vpiSetInt)                                       \
  F(VPISetBigInt, vpiSetBigInt)                                 \
  F(VPISetReal, vpiSetReal)                                     \
  F(VPISetDouble, vpiSetDouble)                                 \
  F(VPISetDate, vpiSetDate)                                     \
  F(VPISetString, vpiSetString)                                 \
  F(VPIFree, vpiFree)                                           \
                                                                \
  /* Compact Storage */                                         \
  F(CompactStorageWriteBool, csWriteBool)                       \
  F(CompactStorageWriteTinyInt, csWriteTinyInt)                 \
  F(CompactStorageWriteSmallInt, csWriteSmallInt)               \
  F(CompactStorageWriteInteger, csWriteInteger)                 \
  F(CompactStorageWriteBigInt, csWriteBigInt)                   \
  F(CompactStorageWriteReal, csWriteReal)                       \
  F(CompactStorageWriteDouble, csWriteDouble)                   \
  F(CompactStorageWriteDate, csWriteDate)                       \
  F(CompactStorageWriteTimestamp, csWriteTimestamp)             \
  F(CompactStorageWriteString, csWriteString)                   \
  F(CompactStorageReadBool, csReadBool)                         \
  F(CompactStorageReadTinyInt, csReadTinyInt)                   \
  F(CompactStorageReadSmallInt, csReadSmallInt)                 \
  F(CompactStorageReadInteger, csReadInteger)                   \
  F(CompactStorageReadBigInt, csReadBigInt)                     \
  F(CompactStorageReadReal, csReadReal)                         \
  F(CompactStorageReadDouble, csReadDouble)                     \
  F(CompactStorageReadDate, csReadDate)                         \
  F(CompactStorageReadTimestamp, csReadTimestamp)               \
  F(CompactStorageReadString, csReadString)                     \
                                                                \
  /* Hashing */                                                 \
  F(Hash, hash)                                                 \
                                                                \
  /* Filter Manager */                                          \
  F(FilterManagerInit, filterManagerInit)                       \
  F(FilterManagerInsertFilter, filterManagerInsertFilter)       \
  F(FilterManagerRunFilters, filterManagerRunFilters)           \
  F(FilterManagerFree, filterManagerFree)                       \
  /* Filter Execution */                                        \
  F(VectorFilterEqual, filterEq)                                \
  F(VectorFilterGreaterThan, filterGt)                          \
  F(VectorFilterGreaterThanEqual, filterGe)                     \
  F(VectorFilterLessThan, filterLt)                             \
  F(VectorFilterLessThanEqual, filterLe)                        \
  F(VectorFilterNotEqual, filterNe)                             \
                                                                \
  /* Aggregations */                                            \
  F(AggHashTableInit, aggHTInit)                                \
  F(AggHashTableInsert, aggHTInsert)                            \
  F(AggHashTableLinkEntry, aggHTLink)                           \
  F(AggHashTableLookup, aggHTLookup)                            \
  F(AggHashTableProcessBatch, aggHTProcessBatch)                \
  F(AggHashTableMovePartitions, aggHTMoveParts)                 \
  F(AggHashTableParallelPartitionedScan, aggHTParallelPartScan) \
  F(AggHashTableFree, aggHTFree)                                \
  F(AggHashTableIterInit, aggHTIterInit)                        \
  F(AggHashTableIterHasNext, aggHTIterHasNext)                  \
  F(AggHashTableIterNext, aggHTIterNext)                        \
  F(AggHashTableIterGetRow, aggHTIterGetRow)                    \
  F(AggHashTableIterClose, aggHTIterClose)                      \
  F(AggPartIterHasNext, aggPartIterHasNext)                     \
  F(AggPartIterNext, aggPartIterNext)                           \
  F(AggPartIterGetHash, aggPartIterGetHash)                     \
  F(AggPartIterGetRow, aggPartIterGetRow)                       \
  F(AggPartIterGetRowEntry, aggPartIterGetRowEntry)             \
  F(AggInit, aggInit)                                           \
  F(AggAdvance, aggAdvance)                                     \
  F(AggMerge, aggMerge)                                         \
  F(AggReset, aggReset)                                         \
  F(AggResult, aggResult)                                       \
                                                                \
  /* Joins */                                                   \
  F(JoinHashTableInit, joinHTInit)                              \
  F(JoinHashTableInsert, joinHTInsert)                          \
  F(JoinHashTableBuild, joinHTBuild)                            \
  F(JoinHashTableBuildParallel, joinHTBuildParallel)            \
  F(JoinHashTableLookup, joinHTLookup)                          \
  F(JoinHashTableFree, joinHTFree)                              \
                                                                \
  /* Hash Table Entry */                                        \
  F(HashTableEntryGetHash, htEntryGetHash)                      \
  F(HashTableEntryGetRow, htEntryGetRow)                        \
  F(HashTableEntryGetNext, htEntryGetNext)                      \
                                                                \
  /* Sorting */                                                 \
  F(SorterInit, sorterInit)                                     \
  F(SorterInsert, sorterInsert)                                 \
  F(SorterInsertTopK, sorterInsertTopK)                         \
  F(SorterInsertTopKFinish, sorterInsertTopKFinish)             \
  F(SorterSort, sorterSort)                                     \
  F(SorterSortParallel, sorterSortParallel)                     \
  F(SorterSortTopKParallel, sorterSortTopKParallel)             \
  F(SorterFree, sorterFree)                                     \
  F(SorterIterInit, sorterIterInit)                             \
  F(SorterIterHasNext, sorterIterHasNext)                       \
  F(SorterIterNext, sorterIterNext)                             \
  F(SorterIterSkipRows, sorterIterSkipRows)                     \
  F(SorterIterGetRow, sorterIterGetRow)                         \
  F(SorterIterClose, sorterIterClose)                           \
                                                                \
  /* Output */                                                  \
  F(ResultBufferAllocOutRow, resultBufferAllocRow)              \
  F(ResultBufferFinalize, resultBufferFinalize)                 \
                                                                \
  /* CSV */                                                     \
  F(CSVReaderInit, csvReaderInit)                               \
  F(CSVReaderAdvance, csvReaderAdvance)                         \
  F(CSVReaderGetField, csvReaderGetField)                       \
  F(CSVReaderGetRecordNumber, csvReaderGetRecordNumber)         \
  F(CSVReaderClose, csvReaderClose)                             \
                                                                \
  /* Trig */                                                    \
  F(ACos, acos)                                                 \
  F(ASin, asin)                                                 \
  F(ATan, atan)                                                 \
  F(ATan2, atan2)                                               \
  F(Cos, cos)                                                   \
  F(Cot, cot)                                                   \
  F(Sin, sin)                                                   \
  F(Tan, tan)                                                   \
                                                                \
  /* Bits */                                                    \
  F(Ctlz, ctlz)                                                 \
  F(Cttz, cttz)                                                 \
                                                                \
  /* Generic */                                                 \
  F(SizeOf, sizeOf)                                             \
  F(OffsetOf, offsetOf)                                         \
  F(IntCast, intCast)                                           \
  F(PtrCast, ptrCast)

/**
 * An enumeration of all TPL builtin functions.
 */
enum class Builtin : uint8_t {
#define ENTRY(Name, ...) Name,
  BUILTINS_LIST(ENTRY)
#undef ENTRY
#define COUNT_OP(inst, ...) +1
      Last = -1 BUILTINS_LIST(COUNT_OP)
#undef COUNT_OP
};

/**
 * Helper class providing.
 */
class Builtins : public AllStatic {
 public:
  // The total number of builtin functions
  static const uint32_t kBuiltinsCount = static_cast<uint32_t>(Builtin::Last) + 1;

  /**
   * @return The total number of builtin functions.
   */
  static constexpr uint32_t NumBuiltins() { return kBuiltinsCount; }

  /**
   * @return The name of the function associated with the given builtin enumeration.
   */
  static const char *GetFunctionName(Builtin builtin) {
    return kBuiltinFunctionNames[static_cast<uint8_t>(builtin)];
  }

 private:
  static const char *kBuiltinFunctionNames[];
};

}  // namespace tpl::ast
