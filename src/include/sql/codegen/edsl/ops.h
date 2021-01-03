#pragma once

#include "ast/type_proxy.h"
#include "sql/codegen/edsl/boolean_ops.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"

namespace tpl::sql::codegen::edsl {

/**
 * Convert a SQL boolean to a primitive boolean.
 * @param val The input SQL boolean value.
 * @return The result of the conversion.
 */
inline Value<bool> ForceTruth(const Value<ast::x::BooleanVal> &val) {
  CodeGen *codegen = val.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::SqlToBool, {val.GetRaw()});
  call->SetType(codegen->BoolType());
  return Value<bool>(codegen, call);
}

/**
 * Specialization for references to sql::ExecutionContext.
 */
template <>
class Reference<ast::x::ExecutionContext> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<ast::x::ThreadStateContainer *> GetThreadStateContainer() const {
    auto tls = codegen_->CallBuiltin(ast::Builtin::ExecutionContextGetTLS, {val_});
    tls->SetType(codegen_->GetType<ast::x::ThreadStateContainer *>());
    return Value<ast::x::ThreadStateContainer *>(codegen_, tls);
  }

  Value<ast::x::MemoryPool *> GetMemoryPool() const {
    auto mem_pool = codegen_->CallBuiltin(ast::Builtin::ExecutionContextGetMemoryPool, {val_});
    mem_pool->SetType(codegen_->GetType<ast::x::MemoryPool *>());
    return Value<ast::x::MemoryPool *>(codegen_, mem_pool);
  }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::ThreadStateContainer.
 */
template <>
class Reference<ast::x::ThreadStateContainer> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<uint8_t *> AccessCurrentThreadState() const {
    auto thread_state = codegen_->CallBuiltin(ast::Builtin::ThreadStateContainerGetState, {val_});
    thread_state->SetType(codegen_->GetType<uint8_t *>());
    return Value<uint8_t *>(codegen_, thread_state);
  }

  Value<void> Iterate(const Value<uint8_t *> &context, ast::Identifier func) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::ThreadStateContainerIterate,
                                      {val_, context.GetRaw(), codegen_->MakeExpr(func)});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Reset(const Value<uint32_t> &state_size, ast::Identifier init_fn,
                    ast::Identifier tear_down_fn, const Value<uint8_t *> &context) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::ThreadStateContainerReset,
                                      {val_, state_size.GetRaw(), codegen_->MakeExpr(init_fn),
                                       codegen_->MakeExpr(tear_down_fn), context.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Clear() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::ThreadStateContainerClear, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to util::CSVReader.
 */
template <>
class Reference<ast::x::CSVReader> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<bool> Init(std::string_view file_name) const {
    auto call =
        codegen_->CallBuiltin(ast::Builtin::CSVReaderInit, {val_, codegen_->Literal(file_name)});
    call->SetType(codegen_->GetType<bool>());
    return Value<bool>(codegen_, call);
  }

  Value<bool> Advance() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::CSVReaderAdvance, {val_});
    call->SetType(codegen_->GetType<bool>());
    return Value<bool>(codegen_, call);
  }

  Value<void> GetField(uint32_t field_index, const Value<ast::x::StringVal *> &result) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::CSVReaderGetField,
                                      {val_, codegen_->Literal(field_index), result.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<uint32_t> GetRecordNumber() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::CSVReaderGetRecordNumber, {val_});
    call->SetType(codegen_->GetType<uint32_t>());
    return Value<uint32_t>(codegen_, call);
  }

  Value<void> Close() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::CSVReaderClose, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::JoinHashTable.
 */
template <>
class Reference<ast::x::JoinHashTable> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<void> Init(const Value<ast::x::MemoryPool *> &mem_pool,
                   const Value<uint32_t> &row_size) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::JoinHashTableInit,
                                      {val_, mem_pool.GetRaw(), row_size.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Init(const Value<ast::x::MemoryPool *> &mem_pool, const Value<uint32_t> &row_size,
                   ast::Identifier analysis_fn, ast::Identifier compress_fn) const {
    auto call =
        codegen_->CallBuiltin(ast::Builtin::JoinHashTableInit,
                              {val_, mem_pool.GetRaw(), row_size.GetRaw(),
                               codegen_->MakeExpr(analysis_fn), codegen_->MakeExpr(compress_fn)});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<uint8_t *> Insert(const Value<hash_t> &hash_val) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::JoinHashTableInsert, {val_, hash_val.GetRaw()});
    call->SetType(codegen_->GetType<uint8_t *>());
    return Value<uint8_t *>(codegen_, call);
  }

  Value<void> Build() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::JoinHashTableBuild, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> BuildParallel(const Value<ast::x::ThreadStateContainer *> &tls,
                            const Value<uint32_t> &offset) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::JoinHashTableBuildParallel,
                                      {val_, tls.GetRaw(), offset.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<ast::x::HashTableEntry *> Lookup(const Value<hash_t> &hash_val) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::JoinHashTableLookup, {val_, hash_val.GetRaw()});
    call->SetType(codegen_->GetType<ast::x::HashTableEntry *>());
    return Value<ast::x::HashTableEntry *>(codegen_, call);
  }

  Value<void> Free() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::JoinHashTableFree, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::HashTableEntry.
 */
template <>
class Reference<ast::x::HashTableEntry> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<hash_t> GetHash() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::HashTableEntryGetHash, {val_});
    call->SetType(codegen_->GetType<hash_t>());
    return Value<hash_t>(codegen_, call);
  }

  Value<uint8_t *> GetRow() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::HashTableEntryGetRow, {val_});
    call->SetType(codegen_->GetType<uint8_t *>());
    return Value<uint8_t *>(codegen_, call);
  }

  Value<ast::x::HashTableEntry *> Next() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::HashTableEntryGetNext, {val_});
    call->SetType(codegen_->GetType<ast::x::HashTableEntry *>());
    return Value<ast::x::HashTableEntry *>(codegen_, call);
  }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::AggregationHashTable.
 */
template <>
class Reference<ast::x::AggregationHashTable> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<void> Init(const Value<ast::x::MemoryPool *> &mem_pool,
                   const Value<uint32_t> &payload_size) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggHashTableInit,
                                      {val_, mem_pool.GetRaw(), payload_size.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<uint8_t *> Lookup(const Value<hash_t> &hash_val, ast::Identifier key_check,
                          const ValueVT &input) const {
    auto call = codegen_->CallBuiltin(
        ast::Builtin::AggHashTableLookup,
        {val_, hash_val.GetRaw(), codegen_->MakeExpr(key_check), input.GetRaw()});
    call->SetType(codegen_->GetType<uint8_t *>());
    return Value<uint8_t *>(codegen_, call);
  }

  Value<uint8_t *> Insert(const Value<hash_t> &hash_val, const Value<bool> &partitioned) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggHashTableInsert,
                                      {val_, hash_val.GetRaw(), partitioned.GetRaw()});
    call->SetType(codegen_->GetType<uint8_t *>());
    return Value<uint8_t *>(codegen_, call);
  }

  Value<void> LinkEntry(const Value<ast::x::HashTableEntry *> &entry) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggHashTableLinkEntry, {val_, entry.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> MovePartitions(const Value<ast::x::ThreadStateContainer *> tls,
                             const Value<uint32_t> &ht_offset,
                             ast::Identifier merge_partitions_fn_name) const {
    auto call = codegen_->CallBuiltin(
        ast::Builtin::AggHashTableMovePartitions,
        {val_, tls.GetRaw(), ht_offset.GetRaw(), codegen_->MakeExpr(merge_partitions_fn_name)});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> ParallelScan(const ValueVT &query_state,
                           const Value<ast::x::ThreadStateContainer *> tls,
                           ast::Identifier worker_fn) const {
    auto call = codegen_->CallBuiltin(
        ast::Builtin::AggHashTableParallelPartitionedScan,
        {val_, query_state.GetRaw(), tls.GetRaw(), codegen_->MakeExpr(worker_fn)});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Free() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggHashTableFree, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::AHTIterator.
 */
template <>
class Reference<ast::x::AHTIterator> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<void> Init(const Value<ast::x::AggregationHashTable *> &agg_ht) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggHashTableIterInit, {val_, agg_ht.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<bool> HasNext() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggHashTableIterHasNext, {val_});
    call->SetType(codegen_->GetType<bool>());
    return Value<bool>(codegen_, call);
  }

  Value<void> Next() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggHashTableIterNext, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<uint8_t *> GetRow() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggHashTableIterGetRow, {val_});
    call->SetType(codegen_->GetType<uint8_t *>());
    return Value<uint8_t *>(codegen_, call);
  }

  Value<void> Close() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggHashTableIterClose, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  CodeGen *GetCodeGen() const noexcept { return codegen_; }

  ast::Expression *GetRaw() const noexcept { return val_; }

  Value<ast::x::AHTIterator *> Addr() const noexcept {
    return {codegen_, codegen_->AddressOf(val_)};
  }

 protected:
  // Used by Variable<>
  Reference(CodeGen *codegen, ast::Identifier name, ast::Type *type)
      : codegen_(codegen), val_(codegen_->MakeExpr(name, type)) {}

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::AHTOverflowIterator.
 */
template <>
class Reference<ast::x::AHTOverflowPartitionIterator> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<bool> HasNext() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggPartIterHasNext, {val_});
    call->SetType(codegen_->GetType<bool>());
    return Value<bool>(codegen_, call);
  }

  Value<void> Next() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggPartIterNext, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<hash_t> GetHash() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggPartIterGetHash, {val_});
    call->SetType(codegen_->GetType<hash_t>());
    return Value<hash_t>(codegen_, call);
  }

  Value<uint8_t *> GetRow() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggPartIterGetRow, {val_});
    call->SetType(codegen_->GetType<uint8_t *>());
    return Value<uint8_t *>(codegen_, call);
  }

  Value<ast::x::HashTableEntry *> GetRowEntry() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::AggPartIterGetRowEntry, {val_});
    call->SetType(codegen_->GetType<ast::x::HashTableEntry *>());
    return Value<ast::x::HashTableEntry *>(codegen_, call);
  }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::TableVectorIterator.
 */
template <>
class Reference<ast::x::TableVectorIterator> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<void> Init(std::string_view table_name) const {
    auto call =
        codegen_->CallBuiltin(ast::Builtin::TableIterInit, {val_, codegen_->Literal(table_name)});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<bool> Advance() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::TableIterAdvance, {val_});
    call->SetType(codegen_->GetType<bool>());
    return Value<bool>(codegen_, call);
  }

  Value<ast::x::VectorProjectionIterator *> GetVPI() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::TableIterGetVPI, {val_});
    call->SetType(codegen_->GetType<ast::x::VectorProjectionIterator *>());
    return Value<ast::x::VectorProjectionIterator *>(codegen_, call);
  }

  Value<void> Close() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::TableIterClose, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  CodeGen *GetCodeGen() const noexcept { return codegen_; }

  ast::Expression *GetRaw() const noexcept { return val_; }

  Value<ast::x::TableVectorIterator *> Addr() const noexcept {
    return {codegen_, codegen_->AddressOf(val_)};
  }

 protected:
  // Used by Variable<>
  Reference(CodeGen *codegen, ast::Identifier name, ast::Type *type)
      : codegen_(codegen), val_(codegen_->MakeExpr(name, type)) {}

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::VectorProjectionIterator.
 */
template <>
class Reference<ast::x::VectorProjectionIterator> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

 public:
  Value<bool> IsFiltered() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::VPIIsFiltered, {val_});
    call->SetType(codegen_->GetType<bool>());
    return Value<bool>(codegen_, call);
  }

  Value<bool> HasNext() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::VPIHasNext, {val_});
    call->SetType(codegen_->GetType<bool>());
    return Value<bool>(codegen_, call);
  }

  Value<void> Advance() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::VPIAdvance, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Reset() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::VPIReset, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Init(const Value<ast::x::VectorProjection *> &vp,
                   const Value<ast::x::TupleIdList *> &tids) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::VPIInit, {val_, vp.GetRaw(), tids.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Match(const Value<bool> &cond) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::VPIMatch, {val_, cond.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  ValueVT Get(sql::TypeId type_id, bool nullable, uint32_t idx) const {
    ast::Builtin builtin;
    ast::Type *ret_type = nullptr;
    switch (type_id) {
      case sql::TypeId::Boolean:
        builtin = ast::Builtin::VPIGetBool;
        ret_type = codegen_->GetType<ast::x::BooleanVal>();
        break;
      case sql::TypeId::TinyInt:
        builtin = ast::Builtin::VPIGetTinyInt;
        ret_type = codegen_->GetType<ast::x::IntegerVal>();
        break;
      case sql::TypeId::SmallInt:
        builtin = ast::Builtin::VPIGetSmallInt;
        ret_type = codegen_->GetType<ast::x::IntegerVal>();
        break;
      case sql::TypeId::Integer:
        builtin = ast::Builtin::VPIGetInt;
        ret_type = codegen_->GetType<ast::x::IntegerVal>();
        break;
      case sql::TypeId::BigInt:
        builtin = ast::Builtin::VPIGetBigInt;
        ret_type = codegen_->GetType<ast::x::IntegerVal>();
        break;
      case sql::TypeId::Float:
        builtin = ast::Builtin::VPIGetReal;
        ret_type = codegen_->GetType<ast::x::RealVal>();
        break;
      case sql::TypeId::Double:
        builtin = ast::Builtin::VPIGetDouble;
        ret_type = codegen_->GetType<ast::x::RealVal>();
        break;
      case sql::TypeId::Date:
        builtin = ast::Builtin::VPIGetDate;
        ret_type = codegen_->GetType<ast::x::DateVal>();
        break;
      case sql::TypeId::Varchar:
        builtin = ast::Builtin::VPIGetString;
        ret_type = codegen_->GetType<ast::x::StringVal>();
        break;
      default:
        UNREACHABLE("Cannot read type from VPI");
    }
    auto call = codegen_->CallBuiltin(builtin, {val_, codegen_->Literal(idx)});
    call->SetType(ret_type);
    return ValueVT(codegen_, call);
  }

  CodeGen *GetCodeGen() const noexcept { return codegen_; }

  ast::Expression *GetRaw() const noexcept { return val_; }

  Value<ast::x::VectorProjectionIterator *> Addr() const noexcept {
    return {codegen_, codegen_->AddressOf(val_)};
  }

 protected:
  // Used by Variable<>
  Reference(CodeGen *codegen, ast::Identifier name, ast::Type *type)
      : codegen_(codegen), val_(codegen_->MakeExpr(name, type)) {}

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::FilterManager.
 */
template <>
class Reference<ast::x::FilterManager> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<void> Init() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::FilterManagerInit, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Free() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::FilterManagerFree, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Insert(const std::vector<ast::Identifier> &clause_fn_names) const {
    std::vector<ast::Expression *> params(1 + clause_fn_names.size());
    params[0] = val_;
    for (uint32_t i = 0; i < clause_fn_names.size(); i++) {
      params[i + 1] = codegen_->MakeExpr(clause_fn_names[i]);
    }
    auto call = codegen_->CallBuiltin(ast::Builtin::FilterManagerInsertFilter, params);
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> RunFilters(const Value<ast::x::VectorProjectionIterator *> &vpi) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::FilterManagerRunFilters, {val_, vpi.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::Sorter.
 */
template <>
class Reference<ast::x::Sorter> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<void> Init(const Value<ast::x::MemoryPool *> &mem_pool, ast::Identifier cmp_func_name,
                   const Value<uint32_t> &row_size) const {
    auto call = codegen_->CallBuiltin(
        ast::Builtin::SorterInit,
        {val_, mem_pool.GetRaw(), codegen_->MakeExpr(cmp_func_name), row_size.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<uint8_t *> Insert() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterInsert, {val_});
    call->SetType(codegen_->GetType<uint8_t *>());
    return Value<uint8_t *>(codegen_, call);
  }

  Value<uint8_t *> InsertTopK(const Value<uint64_t> &top_k) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterInsertTopK, {val_, top_k.GetRaw()});
    call->SetType(codegen_->GetType<uint8_t *>());
    return Value<uint8_t *>(codegen_, call);
  }

  Value<void> InsertTopKFinish(const Value<uint64_t> &top_k) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterInsertTopKFinish, {val_, top_k.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Sort() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterSort, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> SortParallel(const Value<ast::x::ThreadStateContainer *> &tls,
                           const Value<uint32_t> &offset) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterSortParallel,
                                      {val_, tls.GetRaw(), offset.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> SortTopKParallel(const Value<ast::x::ThreadStateContainer *> &tls,
                               const Value<uint32_t> &offset, const Value<uint32_t> &top_k) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterSortTopKParallel,
                                      {val_, tls.GetRaw(), offset.GetRaw(), top_k.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> Free() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterFree, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Specialization for references to sql::SorterIterator.
 */
template <>
class Reference<ast::x::SorterIterator> {
 public:
  Reference(CodeGen *codegen, ast::Expression *val) : codegen_(codegen), val_(val) {}

  Value<void> Init(const Value<ast::x::Sorter *> &sorter) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterIterInit, {val_, sorter.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<bool> HasNext() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterIterHasNext, {val_});
    call->SetType(codegen_->GetType<bool>());
    return Value<bool>(codegen_, call);
  }

  Value<void> Next() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterIterNext, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<void> SkipRows(const Value<uint32_t> &n) const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterIterSkipRows, {val_, n.GetRaw()});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  Value<uint8_t *> GetRow() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterIterGetRow, {val_});
    call->SetType(codegen_->GetType<uint8_t *>());
    return Value<uint8_t *>(codegen_, call);
  }

  Value<void> Close() const {
    auto call = codegen_->CallBuiltin(ast::Builtin::SorterIterClose, {val_});
    call->SetType(codegen_->GetType<void>());
    return Value<void>(codegen_->MakeStatement(call));
  }

  CodeGen *GetCodeGen() const noexcept { return codegen_; }

  ast::Expression *GetRaw() const noexcept { return val_; }

  Value<ast::x::SorterIterator *> Addr() const noexcept {
    return {codegen_, codegen_->AddressOf(val_)};
  }

 protected:
  // Used by Variable<>
  Reference(CodeGen *codegen, ast::Identifier name, ast::Type *type)
      : codegen_(codegen), val_(codegen_->MakeExpr(name, type)) {}

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The expression producing the execution context.
  ast::Expression *val_;
};

/**
 * Perform a parallel table scan.
 * @param table_name The name of the table to scan.
 * @param query_state The global query state to pass onto the work function as the first argument.
 * @param tls The thread-local state container.
 * @param worker_name The name of the worker function.
 * @return The statement.
 */
inline Value<void> IterateTableParallel(std::string_view table_name, const ValueVT &query_state,
                                        const Value<ast::x::ThreadStateContainer *> &tls,
                                        ast::Identifier worker_name) {
  CodeGen *codegen = query_state.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::TableIterParallel,
                                   {codegen->Literal(table_name), query_state.GetRaw(),
                                    tls.GetRaw(), codegen->MakeExpr(worker_name)});
  call->SetType(codegen->GetType<void>());
  return Value<void>(codegen->MakeStatement(call));
}

/**
 *
 * @param vp
 * @param cmp_kind
 * @param col_idx
 * @param filter_val
 * @param tids
 * @return
 */
inline Value<void> VPIFilter(const Value<ast::x::VectorProjection *> &vp,
                             planner::ComparisonKind cmp_kind, uint32_t col_idx,
                             const ValueVT &filter_val, const Value<ast::x::TupleIdList *> &tids) {
  CodeGen *codegen = vp.GetCodeGen();
  ast::Builtin builtin;
  switch (cmp_kind) {
    case planner::ComparisonKind::EQUAL:
      builtin = ast::Builtin::VectorFilterEqual;
      break;
    case planner::ComparisonKind::NOT_EQUAL:
      builtin = ast::Builtin::VectorFilterNotEqual;
      break;
    case planner::ComparisonKind::LESS_THAN:
      builtin = ast::Builtin::VectorFilterLessThan;
      break;
    case planner::ComparisonKind::LESS_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::VectorFilterLessThanEqual;
      break;
    case planner::ComparisonKind::GREATER_THAN:
      builtin = ast::Builtin::VectorFilterGreaterThan;
      break;
    case planner::ComparisonKind::GREATER_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::VectorFilterGreaterThanEqual;
      break;
    default:
      UNREACHABLE("Unsupported vector filter type.");
  }
  auto call = codegen->CallBuiltin(
      builtin, {vp.GetRaw(), codegen->Literal(col_idx), filter_val.GetRaw(), tids.GetRaw()});
  call->SetType(codegen->GetType<void>());
  return Value<void>(codegen->MakeStatement(call));
}

/**
 * Initialize the given SQL value as a SQL NULL.
 * @pre The input value must be a SQL value type. An exception is throw otherwise.
 * @param val The SQL value to set.
 * @return The call.
 */
inline Value<void> InitSqlNull(const ReferenceVT &val) {
  CodeGen *codegen = val.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::InitSqlNull, {val.GetRaw()});
  call->SetType(codegen->NilType());
  return Value<void>(codegen->MakeStatement(call));
}

/**
 * @pre The input value must be a SQL value type. An exception is throw otherwise.
 * @return True if the given input value is a SQL NULL value; false otherwise.
 */
inline Value<bool> IsValNull(const ValueVT &val) {
  CodeGen *codegen = val.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::IsValNull, {val.GetRaw()});
  call->SetType(codegen->BoolType());
  return Value<bool>(codegen, call);
}

/**
 * @pre The input value must be a SQL value type. An exception is throw otherwise.
 * @return True if the input value is not a SQL NULL; false otherwise.
 */
inline Value<bool> IsValNotNull(const ValueVT &val) { return !IsValNull(val); }

/**
 * Convert a primitive boolean value into a SQL boolean value.
 * @param b The constant bool.
 * @return The SQL boolean value.
 */
inline Value<ast::x::BooleanVal> BoolToSql(CodeGen *codegen, bool b) {
  ast::Expression *val = codegen->CallBuiltin(ast::Builtin::BoolToSql, {codegen->Literal<bool>(b)});
  val->SetType(codegen->GetType<ast::x::BooleanVal>());
  return Value<ast::x::BooleanVal>(codegen, val);
}

/**
 * Convert a primitive integer value into a SQL integer value.
 * @param num The number to convert.
 * @return The SQL integer value.
 */
inline Value<ast::x::IntegerVal> IntToSql(CodeGen *codegen, int64_t num) {
  ast::Expression *val = codegen->CallBuiltin(ast::Builtin::IntToSql, {codegen->Literal(num)});
  val->SetType(codegen->GetType<ast::x::IntegerVal>());
  return Value<ast::x::IntegerVal>(codegen, val);
}

/**
 * Convert a primitive floating pointer number into a SQL real value.
 * @param num The number to convert.
 * @return The SQL real value.
 */
inline Value<ast::x::RealVal> FloatToSql(CodeGen *codegen, double num) {
  ast::Expression *val = codegen->CallBuiltin(ast::Builtin::FloatToSql, {codegen->Literal(num)});
  val->SetType(codegen->GetType<ast::x::RealVal>());
  return Value<ast::x::RealVal>(codegen, val);
}

/**
 * Convert the decomposed date value into a SQL date value.
 * @param year The year of the date.
 * @param month The month of the date.
 * @param day The day of the date.
 * @return The SQL boolean value.
 */
inline Value<ast::x::DateVal> DateToSql(CodeGen *codegen, int32_t year, int32_t month,
                                        int32_t day) {
  ast::Expression *val = codegen->CallBuiltin(
      ast::Builtin::DateToSql,
      {codegen->Literal(year), codegen->Literal(month), codegen->Literal(day)});
  val->SetType(codegen->GetType<ast::x::DateVal>());
  return Value<ast::x::DateVal>(codegen, val);
}

/**
 * Convert a string value into a SQL varchar value.
 * @param str The string to convert.
 * @return The SQL varchar value.
 */
inline Value<ast::x::StringVal> StringToSql(CodeGen *codegen, std::string_view str) {
  ast::Expression *val = codegen->CallBuiltin(ast::Builtin::StringToSql, {codegen->Literal(str)});
  val->SetType(codegen->GetType<ast::x::StringVal>());
  return Value<ast::x::StringVal>(codegen, val);
}

/**
 * Perform a SQL cast-conversion of the input value @em input to the provided SQL type @em to_type.
 * @param input The input SQL value to cast.
 * @param to_type The type to cast to.
 * @return The result of the cast.
 */
inline ValueVT ConvertSql(const ValueVT &input, sql::TypeId to_type) {
  CodeGen *codegen = input.GetCodeGen();
  ast::Expression *val = nullptr;
  if (input.GetType()->IsSpecificBuiltin(ast::BuiltinType::BooleanVal)) {
    val = codegen->CallBuiltin(ast::Builtin::ConvertBoolToInteger, {input.GetRaw()});
    val->SetType(codegen->GetType<ast::x::IntegerVal>());
  } else if (input.GetType()->IsSpecificBuiltin(ast::BuiltinType::IntegerVal)) {
    val = codegen->CallBuiltin(ast::Builtin::ConvertIntegerToReal, {input.GetRaw()});
    val->SetType(codegen->GetType<ast::x::RealVal>());
  } else if (input.GetType()->IsSpecificBuiltin(ast::BuiltinType::DateVal)) {
    val = codegen->CallBuiltin(ast::Builtin::ConvertDateToTimestamp, {input.GetRaw()});
    val->SetType(codegen->GetType<ast::x::TimestampVal>());
  } else {
    // Strings.
    switch (to_type) {
      case TypeId::Boolean:
        val = codegen->CallBuiltin(ast::Builtin::ConvertStringToBool, {input.GetRaw()});
        val->SetType(codegen->GetType<ast::x::BooleanVal>());
        break;
      case TypeId::TinyInt:
      case TypeId::SmallInt:
      case TypeId::Integer:
      case TypeId::BigInt:
        val = codegen->CallBuiltin(ast::Builtin::ConvertStringToInt, {input.GetRaw()});
        val->SetType(codegen->GetType<ast::x::IntegerVal>());
        break;
      case TypeId::Float:
      case TypeId::Double:
        val = codegen->CallBuiltin(ast::Builtin::ConvertStringToReal, {input.GetRaw()});
        val->SetType(codegen->GetType<ast::x::RealVal>());
        break;
      case TypeId::Date:
        val = codegen->CallBuiltin(ast::Builtin::ConvertStringToDate, {input.GetRaw()});
        val->SetType(codegen->GetType<ast::x::DateVal>());
        break;
      case TypeId::Timestamp:
        val = codegen->CallBuiltin(ast::Builtin::ConvertStringToTime, {input.GetRaw()});
        val->SetType(codegen->GetType<ast::x::TimestampVal>());
        break;
      default:
        break;
    }
  }
  return ValueVT(codegen, val);
}

/**
 * Perform a SQL LIKE() operation on @em str using the pattern @em pattern.
 * @param str The input string.
 * @param pattern The input pattern.
 * @return The call.
 */
inline Value<ast::x::BooleanVal> Like(const Value<ast::x::StringVal> &str,
                                      const Value<ast::x::StringVal> &pattern) {
  CodeGen *codegen = str.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::Like, {str.GetRaw(), pattern.GetRaw()});
  call->SetType(codegen->GetType<ast::x::BooleanVal>());
  return Value<ast::x::BooleanVal>(codegen, call);
}

/**
 * Perform a SQL NOT LIKE() operation on @em str using the pattern @em pattern.
 * @param str The input string.
 * @param pattern The input pattern.
 * @return The call.
 */
inline Value<ast::x::BooleanVal> NotLike(const Value<ast::x::StringVal> &str,
                           const Value<ast::x::StringVal> &pattern) {
  CodeGen *codegen = str.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::LNot, {Like(str, pattern).GetRaw()});
  call->SetType(codegen->GetType<ast::x::BooleanVal>());
  return Value<ast::x::BooleanVal>(codegen, call);
}

/**
 * Hash all the values in @em values.
 * @param values The values to hash.
 * @return The value of the hash.
 */
inline Value<hash_t> Hash(const std::vector<ValueVT> &values) {
  TPL_ASSERT(!values.empty(), "Keys must be non-empty!");
  CodeGen *codegen = values[0].GetCodeGen();
  // Build the list of raw expressions from the input values.
  std::vector<ast::Expression *> value_expressions;
  value_expressions.reserve(values.size());
  std::ranges::transform(values, std::back_inserter(value_expressions),
                         [](const auto &v) { return v.GetRaw(); });
  // Make the call.
  auto call = codegen->CallBuiltin(ast::Builtin::Hash, value_expressions);
  call->SetType(codegen->GetType<hash_t>());
  return Value<hash_t>(codegen, call);
}

/**
 * Initialize an aggregate using the provided pointer.
 * @param agg The pointer to the aggregate to initialize.
 * @return The initialization statement.
 */
inline Value<void> AggregatorInit(const ValueVT &agg) {
  CodeGen *codegen = agg.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::AggInit, {agg.GetRaw()});
  call->SetType(codegen->NilType());
  return Value<void>(codegen->MakeStatement(call));
}

/**
 * Advance the aggregate @em agg with the new input value @em input.
 * @param agg The aggregate to advance.
 * @param input The input.
 * @return The advancement statement.
 */
inline Value<void> AggregatorAdvance(const ValueVT &agg, const ValueVT &input) {
  CodeGen *codegen = agg.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::AggAdvance, {agg.GetRaw(), input.GetRaw()});
  call->SetType(codegen->NilType());
  return Value<void>(codegen->MakeStatement(call));
}

/**
 * Merge the two input aggregates into the first.
 * @param agg1 The left input to the merge.
 * @param agg2 The right input to the merge.
 * @return The merge statement.
 */
inline Value<void> AggregatorMerge(const ValueVT &agg1, const ValueVT &agg2) {
  CodeGen *codegen = agg1.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::AggMerge, {agg1.GetRaw(), agg2.GetRaw()});
  call->SetType(codegen->NilType());
  return Value<void>(codegen->MakeStatement(call));
}

/**
 * Compute the final result of the provided aggregate.
 * @param agg The aggregate to finalize.
 * @return The final result of the aggregate.
 */
inline ValueVT AggregatorResult(const ValueVT &agg) {
  CodeGen *codegen = agg.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::AggResult, {agg.GetRaw()});
  auto agg_type = agg.GetType()->GetPointeeType()->As<ast::BuiltinType>();
  ast::Type *result_type = nullptr;
  switch (const auto kind = agg_type->GetKind(); kind) {
    case ast::BuiltinType::CountStarAggregate:
    case ast::BuiltinType::CountAggregate:
    case ast::BuiltinType::IntegerSumAggregate:
    case ast::BuiltinType::IntegerMinAggregate:
    case ast::BuiltinType::IntegerMaxAggregate:
      result_type = codegen->GetType<ast::x::IntegerVal>();
      break;
    case ast::BuiltinType::RealSumAggregate:
    case ast::BuiltinType::RealMinAggregate:
    case ast::BuiltinType::RealMaxAggregate:
      result_type = codegen->GetType<ast::x::RealVal>();
      break;
    case ast::BuiltinType::DateMinAggregate:
    case ast::BuiltinType::DateMaxAggregate:
      result_type = codegen->GetType<ast::x::DateVal>();
      break;
    case ast::BuiltinType::StringMinAggregate:
    case ast::BuiltinType::StringMaxAggregate:
      result_type = codegen->GetType<ast::x::StringVal>();
      break;
    case ast::BuiltinType::AvgAggregate:
      result_type = codegen->GetType<ast::x::RealVal>();
      break;
    default:
      UNREACHABLE("Impossible aggregate type.");
  }
  call->SetType(result_type);
  return ValueVT(codegen, call);
}

/**
 * Allocate a row in the output buffer contained in the execution context.
 * @param ctx The execution context.
 * @return A pointer to the allocated row.
 */
inline Value<uint8_t *> ResultBufferAllocOutRow(const Value<ast::x::ExecutionContext *> &ctx) {
  CodeGen *codegen = ctx.GetCodeGen();
  auto val = codegen->CallBuiltin(ast::Builtin::ResultBufferAllocOutRow, {ctx.GetRaw()});
  val->SetType(codegen->GetType<uint8_t *>());
  return Value<uint8_t *>(codegen, val);
}

/**
 * Finalize the output buffer contained in the execution context.
 * @param ctx The execution context.
 * @return A non-expression void-value.
 */
inline Value<void> ResultBufferFinalize(const Value<ast::x::ExecutionContext *> &ctx) {
  CodeGen *codegen = ctx.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::ResultBufferFinalize, {ctx.GetRaw()});
  call->SetType(codegen->GetType<void>());
  return Value<void>(codegen->MakeStatement(call));
}

/**
 * Extract the year from a SQL value value.
 * @param date The input date.
 * @return A SQL integer value for the year of the input date.
 */
inline Value<ast::x::IntegerVal> ExtractYear(const Value<ast::x::DateVal> &date) {
  CodeGen *codegen = date.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::ExtractYear, {date.GetRaw()});
  call->SetType(codegen->GetType<ast::x::IntegerVal>());
  return Value<ast::x::IntegerVal>(codegen, call);
}

/**
 * Compute the acos() of the input value.
 * @param val The input value.
 * @return The result of the function.
 */
inline Value<ast::x::RealVal> ACos(const Value<ast::x::RealVal> &val) {
  CodeGen *codegen = val.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::ACos, {val.GetRaw()});
  call->SetType(codegen->GetType<ast::x::RealVal>());
  return Value<ast::x::RealVal>(codegen, call);
}

/**
 * Compute the asin() of the input value.
 * @param val The input value.
 * @return The result of the function.
 */
inline Value<ast::x::RealVal> ASin(const Value<ast::x::RealVal> &val) {
  CodeGen *codegen = val.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::ASin, {val.GetRaw()});
  call->SetType(codegen->GetType<ast::x::RealVal>());
  return Value<ast::x::RealVal>(codegen, call);
}

/**
 * Compute the atan() of the input value.
 * @param val The input value.
 * @return The result of the function.
 */
inline Value<ast::x::RealVal> ATan(const Value<ast::x::RealVal> &val) {
  CodeGen *codegen = val.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::ATan, {val.GetRaw()});
  call->SetType(codegen->GetType<ast::x::RealVal>());
  return Value<ast::x::RealVal>(codegen, call);
}

/**
 * Compute the cos() of the input value.
 * @param val The input value.
 * @return The result of the function.
 */
inline Value<ast::x::RealVal> Cos(const Value<ast::x::RealVal> &val) {
  CodeGen *codegen = val.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::Cos, {val.GetRaw()});
  call->SetType(codegen->GetType<ast::x::RealVal>());
  return Value<ast::x::RealVal>(codegen, call);
}

/**
 * Compute the cot() of the input value.
 * @param val The input value.
 * @return The result of the function.
 */
inline Value<ast::x::RealVal> Cot(const Value<ast::x::RealVal> &val) {
  CodeGen *codegen = val.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::Cot, {val.GetRaw()});
  call->SetType(codegen->GetType<ast::x::RealVal>());
  return Value<ast::x::RealVal>(codegen, call);
}

/**
 * Compute the sin() of the input value.
 * @param val The input value.
 * @return The result of the function.
 */
inline Value<ast::x::RealVal> Sin(const Value<ast::x::RealVal> &val) {
  CodeGen *codegen = val.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::Sin, {val.GetRaw()});
  call->SetType(codegen->GetType<ast::x::RealVal>());
  return Value<ast::x::RealVal>(codegen, call);
}

/**
 * Compute the tan() of the input value.
 * @param val The input value.
 * @return The result of the function.
 */
inline Value<ast::x::RealVal> Tan(const Value<ast::x::RealVal> &val) {
  CodeGen *codegen = val.GetCodeGen();
  auto call = codegen->CallBuiltin(ast::Builtin::Tan, {val.GetRaw()});
  call->SetType(codegen->GetType<ast::x::RealVal>());
  return Value<ast::x::RealVal>(codegen, call);
}

}  // namespace tpl::sql::codegen::edsl
