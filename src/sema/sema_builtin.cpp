#include "sema/sema.h"

#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "ast/type.h"
#include "ast/type_builder.h"
#include "sema/error_reporter.h"

namespace tpl::sema {

namespace {

bool IsPointerToSpecificBuiltin(ast::Type *type, ast::BuiltinType::Kind kind) {
  if (auto pointee_type = type->GetPointeeType()) {
    return pointee_type->IsSpecificBuiltin(kind);
  }
  return false;
}

bool IsPointerToSQLValue(ast::Type *type) {
  if (auto pointee_type = type->GetPointeeType()) {
    return pointee_type->IsSqlValueType();
  }
  return false;
}

bool IsPointerToAggregatorValue(ast::Type *type) {
  if (auto pointee_type = type->GetPointeeType()) {
    return pointee_type->IsSqlAggregatorType();
  }
  return false;
}

template <typename... ArgTypes>
bool AreAllFunctions(const ArgTypes... type) {
  return (true && ... && type->IsFunctionType());
}

// Represent arguments that should be at least one of the templated types.
template <typename... T>
struct Either;

// Represent a string literal argument.
struct StringLiteral;
// Represent any pointer type.
struct AnyPointer;
// Represent any function type.
struct AnyFunction;
// Byte arrays.
template <typename T>
struct Array;
// Any SQL value.
struct SqlValue;
}  // namespace

template <typename T>
struct Sema::ArgCheck<T> {
  static bool Check(Sema *sema, ast::CallExpr *call, uint32_t index) {
    ast::Type *expected_type = ast::TypeBuilder<T>::Get(sema->context_);
    ast::Expr *arg = call->Arguments()[index];
    // Function application simplifies to performing an assignment of the actual
    // call arguments to the function parameters. Do the check now, which may
    // apply an implicit cast to make the assignment work.
    if (!sema->CheckAssignmentConstraints(expected_type, arg)) {
      sema->ReportIncorrectCallArg(call, index, expected_type);
      return false;
    }
    // If the check applied an implicit cast, set the argument.
    if (arg != call->Arguments()[index]) {
      call->SetArgument(index, arg);
    }
    // All good.
    return true;
  }
};

template <>
struct Sema::ArgCheck<StringLiteral> {
  static bool Check(Sema *sema, ast::CallExpr *call, uint32_t index) {
    if (!call->Arguments()[index]->IsStringLiteral()) {
      sema->ReportIncorrectCallArg(call, index, "string literal");
      return false;
    }
    return true;
  }
};

template <>
struct Sema::ArgCheck<AnyPointer> {
  static bool Check(Sema *sema, ast::CallExpr *call, uint32_t index) {
    if (!call->Arguments()[index]->GetType()->IsPointerType()) {
      sema->ReportIncorrectCallArg(call, index, "pointer");
      return false;
    }
    return true;
  }
};

template <>
struct Sema::ArgCheck<AnyFunction> {
  static bool Check(Sema *sema, ast::CallExpr *call, uint32_t index) {
    if (!call->Arguments()[index]->GetType()->IsFunctionType()) {
      sema->ReportIncorrectCallArg(call, index, "function");
      return false;
    }
    return true;
  }
};

template <typename T>
struct Sema::ArgCheck<Array<T>> : public Sema::ArgCheck<T[]> {};

template <>
struct Sema::ArgCheck<SqlValue> {
  static bool Check(Sema *sema, ast::CallExpr *call, uint32_t index) {
    if (!call->Arguments()[index]->GetType()->IsSqlValueType()) {
      sema->ReportIncorrectCallArg(call, index, "SQL value");
      return false;
    }
    return true;
  }
};

template <typename T, typename... Rest>
struct Sema::ArgCheck<T, Rest...> {
  static bool Check(Sema *sema, ast::CallExpr *call, uint32_t index) {
    return ArgCheck<T>::Check(sema, call, index) && ArgCheck<Rest...>::Check(sema, call, index + 1);
  }
};

template <typename Ret, typename... Args>
struct Sema::CheckHelper<Ret(Args...)> {
  static bool CheckBuiltinCall(Sema *sema, ast::CallExpr *call) {
    // If the argument counts don't match, there's an error.
    if (!sema->CheckArgCount(call, sizeof...(Args))) return false;

    // Check each argument.
    if (!ArgCheck<Args...>::Check(sema, call, 0)) return false;

    // Looks good.
    call->SetType(ast::TypeBuilder<Ret>::Get(sema->context_));
    return true;
  }
};

template <typename T>
bool Sema::GenericBuiltinCheck(ast::CallExpr *call) {
  return CheckHelper<T>::CheckBuiltinCall(this, call);
}

void Sema::CheckSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin) {
  // clang-format off
  switch (builtin) {
    case ast::Builtin::BoolToSql: GenericBuiltinCheck<sql::BoolVal(bool)>(call); break;
    case ast::Builtin::IntToSql: GenericBuiltinCheck<sql::Integer(int32_t)>(call); break;
    case ast::Builtin::FloatToSql: GenericBuiltinCheck<sql::Real(float)>(call); break;
    case ast::Builtin::StringToSql: GenericBuiltinCheck<sql::StringVal(StringLiteral)>(call); break;
    case ast::Builtin::DateToSql: GenericBuiltinCheck<tpl::sql::DateVal(int32_t, int32_t, int32_t)>(call); break;
    case ast::Builtin::SqlToBool: GenericBuiltinCheck<bool(sql::BoolVal)>(call); break;
    case ast::Builtin::ConvertBoolToInteger: GenericBuiltinCheck<sql::Integer(sql::BoolVal)>(call); break;
    case ast::Builtin::ConvertIntegerToReal: GenericBuiltinCheck<sql::Real(sql::Integer)>(call); break;
    case ast::Builtin::ConvertDateToTimestamp: GenericBuiltinCheck<sql::TimestampVal(sql::DateVal)>(call); break;
    case ast::Builtin::ConvertStringToBool: GenericBuiltinCheck<sql::BoolVal(sql::StringVal)>(call); break;
    case ast::Builtin::ConvertStringToInt: GenericBuiltinCheck<sql::Integer(sql::StringVal)>(call); break;
    case ast::Builtin::ConvertStringToReal: GenericBuiltinCheck<sql::Real(sql::StringVal)>(call); break;
    case ast::Builtin::ConvertStringToDate: GenericBuiltinCheck<sql::DateVal(sql::StringVal)>(call); break;
    case ast::Builtin::ConvertStringToTime: GenericBuiltinCheck<sql::TimestampVal(sql::StringVal)>(call); break;
    default: UNREACHABLE("Impossible SQL conversion call");
  }
  // clang-format on
}

void Sema::CheckNullValueCall(ast::CallExpr *call, UNUSED ast::Builtin builtin) {
  if (!CheckArgCount(call, 1)) {
    return;
  }
  // Input must be a SQL value.
  if (auto type = call->Arguments()[0]->GetType(); !type->IsSqlValueType()) {
    ErrorReporter().Report(call->Position(), ErrorMessages::kIsValNullExpectsSqlValue, type);
    return;
  }
  // Returns a primitive boolean.
  call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
}

void Sema::CheckBuiltinStringLikeCall(ast::CallExpr *call) {
  GenericBuiltinCheck<sql::BoolVal(sql::StringVal, sql::StringVal)>(call);
}

void Sema::CheckBuiltinDateFunctionCall(ast::CallExpr *call, UNUSED ast::Builtin builtin) {
  GenericBuiltinCheck<sql::Integer(sql::DateVal)>(call);
}

void Sema::CheckBuiltinConcat(ast::CallExpr *call) {
  if (!CheckArgCountAtLeast(call, 3)) {
    return;
  }
  // First argument is an execution context.
  if (const auto ctx_kind = ast::BuiltinType::Kind::ExecutionContext;
      call->Arguments()[0]->GetType()->IsSpecificBuiltin(ctx_kind)) {
    return;
  }
  // All arguments must be SQL strings.
  for (unsigned i = 1; i < call->Arguments().size(); i++) {
    const auto arg = call->Arguments()[i];
    if (!arg->GetType()->IsSpecificBuiltin(ast::BuiltinType::Kind::StringVal)) {
      error_reporter_->Report(arg->Position(), ErrorMessages::kBadHashArg, arg->GetType());
      return;
    }
  }
  // Result is a string.
  call->SetType(GetBuiltinType(ast::BuiltinType::Kind::StringVal));
}

void Sema::CheckBuiltinAggHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableInit:
      GenericBuiltinCheck<void(sql::AggregationHashTable *, sql::MemoryPool *, uint32_t)>(call);
      break;
    case ast::Builtin::AggHashTableInsert:
      // Distinguish between partitioned and non-partitioned insertion.
      if (call->NumArgs() == 2) {
        GenericBuiltinCheck<byte *(sql::AggregationHashTable *, hash_t)>(call);
      } else {
        GenericBuiltinCheck<byte *(sql::AggregationHashTable *, hash_t, bool)>(call);
      }
      break;
    case ast::Builtin::AggHashTableLinkEntry:
      GenericBuiltinCheck<void(sql::AggregationHashTable *, sql::HashTableEntry *)>(call);
      break;
    case ast::Builtin::AggHashTableLookup:
      GenericBuiltinCheck<byte *(sql::AggregationHashTable *, hash_t, AnyFunction, AnyPointer)>(
          call);
      break;
    case ast::Builtin::AggHashTableProcessBatch:
      GenericBuiltinCheck<void(sql::AggregationHashTable *, sql::VectorProjectionIterator *,
                               uint32_t[], AnyFunction, AnyFunction, bool)>(call);
      break;
    case ast::Builtin::AggHashTableMovePartitions:
      GenericBuiltinCheck<void(sql::AggregationHashTable *, sql::ThreadStateContainer *, uint32_t,
                               AnyFunction)>(call);
      break;
    case ast::Builtin::AggHashTableParallelPartitionedScan:
      GenericBuiltinCheck<void(sql::AggregationHashTable *, AnyPointer, sql::ThreadStateContainer *,
                               AnyFunction)>(call);
      break;
    case ast::Builtin::AggHashTableFree:
      GenericBuiltinCheck<void(sql::AggregationHashTable *)>(call);
      break;
    default:
      UNREACHABLE("Impossible aggregation hash table call");
  }
}

void Sema::CheckBuiltinAggHashTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableIterInit:
      GenericBuiltinCheck<void(sql::AHTIterator *, const sql::AggregationHashTable *)>(call);
      break;
    case ast::Builtin::AggHashTableIterHasNext:
      GenericBuiltinCheck<bool(sql::AHTIterator *)>(call);
      break;
    case ast::Builtin::AggHashTableIterNext:
      GenericBuiltinCheck<void(sql::AHTIterator *)>(call);
      break;
    case ast::Builtin::AggHashTableIterGetRow:
      GenericBuiltinCheck<byte *(sql::AHTIterator *)>(call);
      break;
    case ast::Builtin::AggHashTableIterClose:
      GenericBuiltinCheck<void(sql::AHTIterator *)>(call);
      break;
    default:
      UNREACHABLE("Impossible aggregation hash table iterator call");
  }
}

void Sema::CheckBuiltinAggPartIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggPartIterHasNext:
      GenericBuiltinCheck<bool(sql::AHTOverflowPartitionIterator *)>(call);
      break;
    case ast::Builtin::AggPartIterNext:
      GenericBuiltinCheck<void(sql::AHTOverflowPartitionIterator *)>(call);
      break;
    case ast::Builtin::AggPartIterGetRowEntry:
      GenericBuiltinCheck<sql::HashTableEntry *(sql::AHTOverflowPartitionIterator *)>(call);
      break;
    case ast::Builtin::AggPartIterGetRow:
      GenericBuiltinCheck<byte *(sql::AHTOverflowPartitionIterator *)>(call);
      break;
    case ast::Builtin::AggPartIterGetHash:
      GenericBuiltinCheck<hash_t(sql::AHTOverflowPartitionIterator *)>(call);
      break;
    default:
      UNREACHABLE("Impossible aggregation partition iterator call");
  }
}

void Sema::CheckBuiltinAggregatorCall(ast::CallExpr *call, ast::Builtin builtin) {
  const auto &args = call->Arguments();
  switch (builtin) {
    case ast::Builtin::AggInit:
    case ast::Builtin::AggReset: {
      // All arguments to @aggInit() or @aggReset() must be SQL aggregators
      for (uint32_t idx = 0; idx < call->NumArgs(); idx++) {
        if (!IsPointerToAggregatorValue(args[idx]->GetType())) {
          error_reporter_->Report(call->Position(), ErrorMessages::kNotASQLAggregate,
                                  args[idx]->GetType());
          return;
        }
      }
      // Init returns nil
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggAdvance: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // First argument to @aggAdvance() must be a SQL aggregator, second must be a SQL value
      if (!IsPointerToAggregatorValue(args[0]->GetType())) {
        error_reporter_->Report(call->Position(), ErrorMessages::kNotASQLAggregate,
                                args[0]->GetType());
        return;
      }
      if (!IsPointerToSQLValue(args[1]->GetType())) {
        error_reporter_->Report(call->Position(), ErrorMessages::kNotASQLAggregate,
                                args[1]->GetType());
        return;
      }
      // Advance returns nil
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggMerge: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // Both arguments must be SQL aggregators
      bool arg0_is_agg = IsPointerToAggregatorValue(args[0]->GetType());
      bool arg1_is_agg = IsPointerToAggregatorValue(args[1]->GetType());
      if (!arg0_is_agg || !arg1_is_agg) {
        error_reporter_->Report(call->Position(), ErrorMessages::kNotASQLAggregate,
                                (!arg0_is_agg ? args[0]->GetType() : args[1]->GetType()));
        return;
      }
      // Merge returns nil
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggResult: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      // Argument must be a SQL aggregator
      if (!IsPointerToAggregatorValue(args[0]->GetType())) {
        error_reporter_->Report(call->Position(), ErrorMessages::kNotASQLAggregate,
                                args[0]->GetType());
        return;
      }
      switch (args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind()) {
        case ast::BuiltinType::Kind::CountAggregate:
        case ast::BuiltinType::Kind::CountStarAggregate:
        case ast::BuiltinType::Kind::IntegerMaxAggregate:
        case ast::BuiltinType::Kind::IntegerMinAggregate:
        case ast::BuiltinType::Kind::IntegerSumAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::IntegerVal));
          break;
        case ast::BuiltinType::Kind::RealMaxAggregate:
        case ast::BuiltinType::Kind::RealMinAggregate:
        case ast::BuiltinType::Kind::RealSumAggregate:
        case ast::BuiltinType::Kind::AvgAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::RealVal));
          break;
        default:
          UNREACHABLE("Impossible aggregate type!");
      }
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregator call");
    }
  }
}

void Sema::CheckBuiltinJoinHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::JoinHashTableInit:
      GenericBuiltinCheck<void(sql::JoinHashTable *, sql::MemoryPool *, uint32_t)>(call);
      break;
    case ast::Builtin::JoinHashTableInsert:
      GenericBuiltinCheck<byte *(sql::JoinHashTable *, hash_t)>(call);
      break;
    case ast::Builtin::JoinHashTableBuild:
      GenericBuiltinCheck<void(sql::JoinHashTable *)>(call);
      break;
    case ast::Builtin::JoinHashTableBuildParallel:
      GenericBuiltinCheck<void(sql::JoinHashTable *, sql::ThreadStateContainer *, uint32_t)>(call);
      break;
    case ast::Builtin::JoinHashTableLookup:
      GenericBuiltinCheck<sql::HashTableEntry *(const sql::JoinHashTable *, hash_t)>(call);
      break;
    case ast::Builtin::JoinHashTableFree:
      GenericBuiltinCheck<void(sql::JoinHashTable *)>(call);
      break;
    default:
      UNREACHABLE("Impossible join hash table build call");
  }
}

void Sema::CheckBuiltinHashTableEntryCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::HashTableEntryGetHash:
      GenericBuiltinCheck<hash_t(const sql::HashTableEntry *)>(call);
      break;
    case ast::Builtin::HashTableEntryGetRow:
      GenericBuiltinCheck<byte *(const sql::HashTableEntry *)>(call);
      break;
    case ast::Builtin::HashTableEntryGetNext:
      GenericBuiltinCheck<sql::HashTableEntry *(const sql::HashTableEntry *)>(call);
      break;
    default:
      UNREACHABLE("Impossible hash table entry iterator call");
  }
}

void Sema::CheckBuiltinExecutionContextCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::ExecutionContextGetMemoryPool:
      GenericBuiltinCheck<sql::MemoryPool *(sql::ExecutionContext *)>(call);
      break;
    case ast::Builtin::ExecutionContextGetTLS:
      GenericBuiltinCheck<sql::ThreadStateContainer *(sql::ExecutionContext *)>(call);
      break;
    default:
      UNREACHABLE("Impossible execution context call");
  }
}

void Sema::CheckBuiltinThreadStateContainerCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // First argument must be thread state container pointer
  auto tls_kind = ast::BuiltinType::ThreadStateContainer;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), tls_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(tls_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::ThreadStateContainerClear: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ThreadStateContainerGetState: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
      break;
    }
    case ast::Builtin::ThreadStateContainerReset: {
      if (!CheckArgCount(call, 5)) {
        return;
      }
      // Second argument must be an integer size of the state
      const auto uint_kind = ast::BuiltinType::Uint32;
      if (!call_args[1]->GetType()->IsSpecificBuiltin(uint_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(uint_kind));
        return;
      }
      // Third and fourth arguments must be functions
      // TODO(pmenon): More thorough check
      if (!AreAllFunctions(call_args[2]->GetType(), call_args[3]->GetType())) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // Fifth argument must be a pointer to something or nil
      if (!call_args[4]->GetType()->IsPointerType() && !call_args[4]->GetType()->IsNilType()) {
        ReportIncorrectCallArg(call, 4, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ThreadStateContainerIterate: {
      if (!CheckArgCount(call, 3)) {
        return;
      }
      // Second argument is a pointer to some context
      if (!call_args[1]->GetType()->IsPointerType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // Third argument is the iteration function callback
      if (!call_args[2]->GetType()->IsFunctionType()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void Sema::CheckBuiltinTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  const auto &call_args = call->Arguments();

  const auto tvi_kind = ast::BuiltinType::TableVectorIterator;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), tvi_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(tvi_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::TableIterInit: {
      // The second argument is the table name as a literal string
      if (!call_args[1]->IsStringLiteral()) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(context_));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::TableIterAdvance: {
      // A single-arg builtin returning a boolean
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::TableIterGetVPI: {
      // A single-arg builtin return a pointer to the current VPI
      const auto vpi_kind = ast::BuiltinType::VectorProjectionIterator;
      call->SetType(GetBuiltinType(vpi_kind)->PointerTo());
      break;
    }
    case ast::Builtin::TableIterClose: {
      // A single-arg builtin returning void
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void Sema::CheckBuiltinTableIterParCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 4)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // First argument is table name as a string literal
  if (!call_args[0]->IsStringLiteral()) {
    ReportIncorrectCallArg(call, 0, ast::StringType::Get(context_));
    return;
  }

  // Second argument is an opaque query state. For now, check it's a pointer.
  const auto void_kind = ast::BuiltinType::Nil;
  if (!call_args[1]->GetType()->IsPointerType()) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(void_kind)->PointerTo());
    return;
  }

  // Third argument is the thread state container
  const auto tls_kind = ast::BuiltinType::ThreadStateContainer;
  if (!IsPointerToSpecificBuiltin(call_args[2]->GetType(), tls_kind)) {
    ReportIncorrectCallArg(call, 2, GetBuiltinType(tls_kind)->PointerTo());
    return;
  }

  // Third argument is scanner function
  auto *scan_fn_type = call_args[3]->GetType()->SafeAs<ast::FunctionType>();
  if (scan_fn_type == nullptr) {
    error_reporter_->Report(call->Position(), ErrorMessages::kBadParallelScanFunction,
                            call_args[3]->GetType());
    return;
  }
  // Check type
  const auto tvi_kind = ast::BuiltinType::TableVectorIterator;
  const auto &params = scan_fn_type->GetParams();
  if (params.size() != 3 || !params[0].type->IsPointerType() || !params[1].type->IsPointerType() ||
      !IsPointerToSpecificBuiltin(params[2].type, tvi_kind)) {
    error_reporter_->Report(call->Position(), ErrorMessages::kBadParallelScanFunction,
                            call_args[3]->GetType());
    return;
  }

  // Nil
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinVPICall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // The first argument must be a *VPI
  const auto vpi_kind = ast::BuiltinType::VectorProjectionIterator;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), vpi_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(vpi_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::VPIInit:
      GenericBuiltinCheck<void(sql::VectorProjectionIterator *, sql::VectorProjection *,
                               sql::TupleIdList *)>(call);
      break;
    case ast::Builtin::VPIFree:
      GenericBuiltinCheck<void(sql::VectorProjectionIterator *)>(call);
      break;
    case ast::Builtin::VPIIsFiltered:
    case ast::Builtin::VPIHasNext:
    case ast::Builtin::VPIAdvance:
    case ast::Builtin::VPIReset:
      GenericBuiltinCheck<bool(sql::VectorProjectionIterator *)>(call);
      break;
    case ast::Builtin::VPIGetSelectedRowCount:
      GenericBuiltinCheck<uint32_t(sql::VectorProjectionIterator *)>(call);
      break;
    case ast::Builtin ::VPIGetVectorProjection:
      GenericBuiltinCheck<sql::VectorProjection *(sql::VectorProjectionIterator *)>(call);
      break;
    case ast::Builtin::VPISetPosition:
      GenericBuiltinCheck<bool(sql::VectorProjectionIterator *, uint32_t)>(call);
      break;
    case ast::Builtin::VPIMatch: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // If the match argument is a SQL boolean, implicitly cast to native
      ast::Expr *match_arg = call_args[1];
      if (match_arg->GetType()->IsSpecificBuiltin(ast::BuiltinType::BooleanVal)) {
        match_arg = ImplCastExprToType(match_arg, GetBuiltinType(ast::BuiltinType::Bool),
                                       ast::CastKind::SqlBoolToBool);
        call->SetArgument(1, match_arg);
      }
      // If the match argument isn't a native boolean , error
      if (!match_arg->GetType()->IsBoolType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Bool));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::VPIGetBool:
      GenericBuiltinCheck<sql::BoolVal(sql::VectorProjectionIterator *, uint32_t)>(call);
      break;
    case ast::Builtin::VPIGetTinyInt:
    case ast::Builtin::VPIGetSmallInt:
    case ast::Builtin::VPIGetInt:
    case ast::Builtin::VPIGetBigInt:
      GenericBuiltinCheck<sql::Integer(sql::VectorProjectionIterator *, uint32_t)>(call);
      break;
    case ast::Builtin::VPIGetReal:
    case ast::Builtin::VPIGetDouble:
      GenericBuiltinCheck<sql::Real(sql::VectorProjectionIterator *, uint32_t)>(call);
      break;
    case ast::Builtin::VPIGetDate:
      GenericBuiltinCheck<sql::DateVal(sql::VectorProjectionIterator *, uint32_t)>(call);
      break;
    case ast::Builtin::VPIGetString:
      GenericBuiltinCheck<sql::StringVal(sql::VectorProjectionIterator *, uint32_t)>(call);
      break;
    case ast::Builtin::VPIGetPointer:
      GenericBuiltinCheck<byte *(sql::VectorProjectionIterator *, uint32_t)>(call);
      break;
    case ast::Builtin::VPISetTinyInt:
    case ast::Builtin::VPISetSmallInt:
    case ast::Builtin::VPISetInt:
    case ast::Builtin::VPISetBigInt:
      GenericBuiltinCheck<void(sql::VectorProjectionIterator *, sql::Integer, uint32_t)>(call);
      break;
    case ast::Builtin::VPISetReal:
    case ast::Builtin::VPISetDouble:
      GenericBuiltinCheck<void(sql::VectorProjectionIterator *, sql::Real, uint32_t)>(call);
      break;
    case ast::Builtin::VPISetDate:
      GenericBuiltinCheck<void(sql::VectorProjectionIterator *, sql::DateVal, uint32_t)>(call);
      break;
    case ast::Builtin::VPISetString:
      GenericBuiltinCheck<void(sql::VectorProjectionIterator *, sql::StringVal, uint32_t)>(call);
      break;
    default:
      UNREACHABLE("Impossible VPI call");
  }
}

void Sema::CheckBuiltinCompactStorageWriteCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::CompactStorageWriteBool:
      GenericBuiltinCheck<void(bool *, Array<byte>, uint32_t, sql::BoolVal)>(call);
      break;
    case ast::Builtin::CompactStorageWriteTinyInt:
      GenericBuiltinCheck<void(int8_t *, Array<byte>, uint32_t, sql::Integer)>(call);
      break;
    case ast::Builtin::CompactStorageWriteSmallInt:
      GenericBuiltinCheck<void(int16_t *, Array<byte>, uint32_t, sql::Integer)>(call);
      break;
    case ast::Builtin::CompactStorageWriteInteger:
      GenericBuiltinCheck<void(int32_t *, Array<byte>, uint32_t, sql::Integer)>(call);
      break;
    case ast::Builtin::CompactStorageWriteBigInt:
      GenericBuiltinCheck<void(int64_t *, Array<byte>, uint32_t, sql::Integer)>(call);
      break;
    case ast::Builtin::CompactStorageWriteReal:
      GenericBuiltinCheck<void(float *, Array<byte>, uint32_t, sql::Real)>(call);
      break;
    case ast::Builtin::CompactStorageWriteDouble:
      GenericBuiltinCheck<void(double *, Array<byte>, uint32_t, sql::Real)>(call);
      break;
    case ast::Builtin::CompactStorageWriteDate:
      GenericBuiltinCheck<void(sql::Date *, Array<byte>, uint32_t, sql::DateVal)>(call);
      break;
    case ast::Builtin::CompactStorageWriteTimestamp:
      GenericBuiltinCheck<void(sql::Timestamp *, Array<byte>, uint32_t, sql::TimestampVal)>(call);
      break;
    case ast::Builtin::CompactStorageWriteString:
      GenericBuiltinCheck<void(sql::VarlenEntry *, Array<byte>, uint32_t, sql::StringVal)>(call);
      break;
    default:
      UNREACHABLE("Impossible CompactStorage::Write() call!");
  }
}

void Sema::CheckBuiltinCompactStorageReadCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::CompactStorageReadBool:
      GenericBuiltinCheck<sql::BoolVal(bool *, Array<byte>, uint32_t)>(call);
      break;
    case ast::Builtin::CompactStorageReadTinyInt:
      GenericBuiltinCheck<sql::Integer(int8_t *, Array<byte>, uint32_t)>(call);
      break;
    case ast::Builtin::CompactStorageReadSmallInt:
      GenericBuiltinCheck<sql::Integer(int16_t *, Array<byte>, uint32_t)>(call);
      break;
    case ast::Builtin::CompactStorageReadInteger:
      GenericBuiltinCheck<sql::Integer(int32_t *, Array<byte>, uint32_t)>(call);
      break;
    case ast::Builtin::CompactStorageReadBigInt:
      GenericBuiltinCheck<sql::Integer(int64_t *, Array<byte>, uint32_t)>(call);
      break;
    case ast::Builtin::CompactStorageReadReal:
      GenericBuiltinCheck<sql::Real(float *, Array<byte>, uint32_t)>(call);
      break;
    case ast::Builtin::CompactStorageReadDouble:
      GenericBuiltinCheck<sql::Real(double *, Array<byte>, uint32_t)>(call);
      break;
    case ast::Builtin::CompactStorageReadDate:
      GenericBuiltinCheck<sql::DateVal(sql::Date *, Array<byte>, uint32_t)>(call);
      break;
    case ast::Builtin::CompactStorageReadTimestamp:
      GenericBuiltinCheck<sql::TimestampVal(sql::Timestamp *, Array<byte>, uint32_t)>(call);
      break;
    case ast::Builtin::CompactStorageReadString:
      GenericBuiltinCheck<sql::StringVal(sql::VarlenEntry *, Array<byte>, uint32_t)>(call);
      break;
    default:
      UNREACHABLE("Impossible CompactStorage::Read() call!");
  }
}

void Sema::CheckBuiltinHashCall(ast::CallExpr *call, UNUSED ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  // All arguments must be SQL types
  for (const auto &arg : call->Arguments()) {
    if (!arg->GetType()->IsSqlValueType()) {
      error_reporter_->Report(arg->Position(), ErrorMessages::kBadHashArg, arg->GetType());
      return;
    }
  }

  // Result is a hash value
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint64));
}

void Sema::CheckBuiltinFilterManagerCall(ast::CallExpr *const call, const ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  // The first argument must be a *FilterManagerBuilder
  const auto fm_kind = ast::BuiltinType::FilterManager;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), fm_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(fm_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::FilterManagerInit:
    case ast::Builtin::FilterManagerFree: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::FilterManagerInsertFilter: {
      for (uint32_t arg_idx = 1; arg_idx < call->NumArgs(); arg_idx++) {
        const auto vector_proj_kind = ast::BuiltinType::VectorProjection;
        const auto tid_list_kind = ast::BuiltinType::TupleIdList;
        auto *arg_type = call->Arguments()[arg_idx]->GetType()->SafeAs<ast::FunctionType>();
        if (arg_type == nullptr || arg_type->GetNumParams() != 3 ||
            !IsPointerToSpecificBuiltin(arg_type->GetParams()[0].type, vector_proj_kind) ||
            !IsPointerToSpecificBuiltin(arg_type->GetParams()[1].type, tid_list_kind) ||
            !arg_type->GetParams()[2].type->IsPointerType()) {
          ReportIncorrectCallArg(call, arg_idx, "(*VectorProjection, *TupleIdList, *uint8)->nil");
          return;
        }
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::FilterManagerRunFilters: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      const auto vpi_kind = ast::BuiltinType::VectorProjectionIterator;
      if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), vpi_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(vpi_kind)->PointerTo());
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible FilterManager call");
    }
  }
}

void Sema::CheckBuiltinVectorFilterCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 4)) {
    return;
  }

  // The first argument must be a *VectorProjection
  const auto vector_proj_kind = ast::BuiltinType::VectorProjection;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), vector_proj_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(vector_proj_kind)->PointerTo());
    return;
  }

  // Second argument is the column index
  const auto &call_args = call->Arguments();
  const auto int32_kind = ast::BuiltinType::Int32;
  const auto uint32_kind = ast::BuiltinType::Uint32;
  if (!call_args[1]->GetType()->IsSpecificBuiltin(int32_kind) &&
      !call_args[1]->GetType()->IsSpecificBuiltin(uint32_kind)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(int32_kind));
    return;
  }

  // Third argument is either an integer or a pointer to a generic value
  if (!call_args[2]->GetType()->IsSpecificBuiltin(int32_kind) &&
      !call_args[2]->GetType()->IsSqlValueType()) {
    ReportIncorrectCallArg(call, 2, GetBuiltinType(int32_kind));
    return;
  }

  // Fourth and last argument is the *TupleIdList
  const auto tid_list_kind = ast::BuiltinType::TupleIdList;
  if (!IsPointerToSpecificBuiltin(call_args[3]->GetType(), tid_list_kind)) {
    ReportIncorrectCallArg(call, 3, GetBuiltinType(tid_list_kind)->PointerTo());
    return;
  }

  // Done
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckMathTrigCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::ATan2:
      GenericBuiltinCheck<sql::Real(sql::Real, sql::Real)>(call);
      break;
    case ast::Builtin::Cos:
    case ast::Builtin::Cot:
    case ast::Builtin::Sin:
    case ast::Builtin::Tan:
    case ast::Builtin::ACos:
    case ast::Builtin::ASin:
    case ast::Builtin::ATan:
      GenericBuiltinCheck<sql::Real(sql::Real)>(call);
      break;
    default:
      UNREACHABLE("Impossible math trig function call");
  }
}

void Sema::CheckResultBufferCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  const auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  if (builtin == ast::Builtin::ResultBufferAllocOutRow) {
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  } else {
    call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
  }
}

void Sema::CheckCSVReaderCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // First argument must be a *CSVReader.
  const auto csv_reader = ast::BuiltinType::CSVReader;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), csv_reader)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(csv_reader));
    return;
  }

  switch (builtin) {
    case ast::Builtin::CSVReaderInit: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // Second argument is either a raw string, or a string representing the
      // name of the CSV file to read. At this stage, we don't care. It just
      // needs to be a string.
      if (!call_args[1]->GetType()->IsStringType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(csv_reader));
        return;
      }

      // Third, fourth, and fifth must be characters.

      // Returns boolean indicating if initialization succeeded.
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::CSVReaderAdvance: {
      // Returns a boolean indicating if there's more data.
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::CSVReaderGetField: {
      if (!CheckArgCount(call, 3)) {
        return;
      }
      // Second argument must be the index, third is a pointer to a SQL string.
      if (!call_args[1]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint32));
      }
      // Second argument must be the index, third is a pointer to a SQL string.
      const auto string_kind = ast::BuiltinType::StringVal;
      if (!IsPointerToSpecificBuiltin(call_args[2]->GetType(), string_kind)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(string_kind)->PointerTo());
      }
      // Returns nothing.
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::CSVReaderGetRecordNumber: {
      // Returns a 32-bit number indicating the current record number.
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
      break;
    }
    case ast::Builtin::CSVReaderClose: {
      // Returns nothing.
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default:
      UNREACHABLE("Impossible math trig function call");
  }
}

void Sema::CheckBuiltinSizeOfCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  // This call returns an unsigned 32-bit value for the size of the type
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
}

void Sema::CheckBuiltinOffsetOfCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 2)) {
    return;
  }

  // First argument must be a resolved composite type
  auto *type = Resolve(call->Arguments()[0]);
  if (type == nullptr || !type->IsStructType()) {
    ReportIncorrectCallArg(call, 0, "composite");
    return;
  }

  // Second argument must be an identifier expression
  auto field = call->Arguments()[1]->SafeAs<ast::IdentifierExpr>();
  if (field == nullptr) {
    ReportIncorrectCallArg(call, 1, "identifier expression");
    return;
  }

  // Field with the given name must exist in the composite type
  if (type->As<ast::StructType>()->LookupFieldByName(field->Name()) == nullptr) {
    error_reporter_->Report(call->Position(), ErrorMessages::kFieldObjectDoesNotExist,
                            field->Name(), type);
    return;
  }

  // Returns a 32-bit value for the offset of the type
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
}

void Sema::CheckBuiltinPtrCastCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 2)) {
    return;
  }

  // The first argument will be a UnaryOpExpr with the '*' (star) op. This is
  // because parsing function calls assumes expression arguments, not types. So,
  // something like '*Type', which would be the first argument to @ptrCast, will
  // get parsed as a dereference expression before a type expression.
  // TODO(pmenon): Fix the above to parse correctly

  auto unary_op = call->Arguments()[0]->SafeAs<ast::UnaryOpExpr>();
  if (unary_op == nullptr || unary_op->Op() != parsing::Token::Type::STAR) {
    error_reporter_->Report(call->Position(), ErrorMessages::kBadArgToPtrCast,
                            call->Arguments()[0]->GetType(), 1);
    return;
  }

  // Replace the unary with a PointerTypeRepr node and resolve it
  call->SetArgument(0, context_->GetNodeFactory()->NewPointerType(call->Arguments()[0]->Position(),
                                                                  unary_op->Input()));

  for (auto *arg : call->Arguments()) {
    auto *resolved_type = Resolve(arg);
    if (resolved_type == nullptr) {
      return;
    }
  }

  // Both arguments must be pointer types
  if (!call->Arguments()[0]->GetType()->IsPointerType() ||
      !call->Arguments()[1]->GetType()->IsPointerType()) {
    error_reporter_->Report(call->Position(), ErrorMessages::kBadArgToPtrCast,
                            call->Arguments()[0]->GetType(), 1);
    return;
  }

  // Apply the cast
  call->SetType(call->Arguments()[0]->GetType());
}

void Sema::CheckBuiltinIntCast(ast::CallExpr *call) {
  // This function is expected to be called BEFORE resolving arguments.

  if (!CheckArgCount(call, 2)) {
    return;
  }

  // The first argument must be an identifier of a primitive integer type.
  auto type_expr = call->Arguments()[0]->SafeAs<ast::IdentifierExpr>();
  if (type_expr == nullptr) {
    error_reporter_->Report(call->Position(), ErrorMessages::kBadArgToIntCast);
    return;
  }
  ast::Type *type = context_->LookupBuiltinType(type_expr->Name());
  if (!type->IsIntegerType()) {
    error_reporter_->Report(call->Position(), ErrorMessages::kBadArgToIntCast1, type_expr->Name());
    return;
  }

  // Resolve input expression.
  ast::Type *resolved_type = Resolve(call->Arguments()[1]);
  if (resolved_type == nullptr) {
    return;
  }
  if (!resolved_type->IsIntegerType()) {
    error_reporter_->Report(call->Position(), ErrorMessages::kBadArgToIntCast2, resolved_type);
    return;
  }

  // All good: we'll cast from resolve_type to type.
  call->SetType(type);
}

void Sema::CheckBuiltinSorterInit(ast::CallExpr *call) {
  if (!CheckArgCount(call, 4)) {
    return;
  }

  const auto &args = call->Arguments();

  // First argument must be a pointer to a Sorter
  const auto sorter_kind = ast::BuiltinType::Sorter;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), sorter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_kind)->PointerTo());
    return;
  }

  // Second argument must be a pointer to a MemoryPool
  const auto mem_kind = ast::BuiltinType::MemoryPool;
  if (!IsPointerToSpecificBuiltin(args[1]->GetType(), mem_kind)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(mem_kind)->PointerTo());
    return;
  }

  // Second argument must be a function
  auto *const cmp_func_type = args[2]->GetType()->SafeAs<ast::FunctionType>();
  if (cmp_func_type == nullptr || cmp_func_type->GetNumParams() != 2 ||
      !cmp_func_type->GetReturnType()->IsSpecificBuiltin(ast::BuiltinType::Bool) ||
      !cmp_func_type->GetParams()[0].type->IsPointerType() ||
      !cmp_func_type->GetParams()[1].type->IsPointerType()) {
    error_reporter_->Report(call->Position(), ErrorMessages::kBadComparisonFunctionForSorter,
                            args[2]->GetType());
    return;
  }

  // Third and last argument must be a 32-bit number representing the tuple size
  const auto uint_kind = ast::BuiltinType::Uint32;
  if (!args[3]->GetType()->IsSpecificBuiltin(uint_kind)) {
    ReportIncorrectCallArg(call, 3, GetBuiltinType(uint_kind));
    return;
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinSorterInsert(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  // First argument must be a pointer to a Sorter
  const auto sorter_kind = ast::BuiltinType::Sorter;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), sorter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_kind)->PointerTo());
    return;
  }

  // If it's an insertion for Top-K, the second argument must be an unsigned integer.
  if (builtin == ast::Builtin::SorterInsertTopK ||
      builtin == ast::Builtin::SorterInsertTopKFinish) {
    if (!CheckArgCount(call, 2)) {
      return;
    }

    // Error if the top-k argument isn't an integer
    ast::Type *uint_type = GetBuiltinType(ast::BuiltinType::Uint32);
    if (!call->Arguments()[1]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 1, uint_type);
      return;
    }
    if (call->Arguments()[1]->GetType() != uint_type) {
      call->SetArgument(
          1, ImplCastExprToType(call->Arguments()[1], uint_type, ast::CastKind::IntegralCast));
    }
  } else {
    // Regular sorter insert, expect one argument.
    if (!CheckArgCount(call, 1)) {
      return;
    }
  }

  // This call returns a pointer to the allocated tuple
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
}

void Sema::CheckBuiltinSorterSort(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // First argument must be a pointer to a Sorter
  const auto sorter_kind = ast::BuiltinType::Sorter;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), sorter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::SorterSort: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      break;
    }
    case ast::Builtin::SorterSortParallel:
    case ast::Builtin::SorterSortTopKParallel: {
      // Second argument is the *ThreadStateContainer.
      const auto tls_kind = ast::BuiltinType::ThreadStateContainer;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), tls_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(tls_kind)->PointerTo());
        return;
      }

      // Third argument must be a 32-bit integer representing the offset.
      ast::Type *uint_type = GetBuiltinType(ast::BuiltinType::Uint32);
      if (call_args[2]->GetType() != uint_type) {
        ReportIncorrectCallArg(call, 2, uint_type);
        return;
      }

      // If it's for top-k, the last argument must be the top-k value
      if (builtin == ast::Builtin::SorterSortParallel) {
        if (!CheckArgCount(call, 3)) {
          return;
        }
      } else {
        if (!CheckArgCount(call, 4)) {
          return;
        }
        if (!call_args[3]->GetType()->IsIntegerType()) {
          ReportIncorrectCallArg(call, 3, uint_type);
          return;
        }
        if (call_args[3]->GetType() != uint_type) {
          call->SetArgument(
              3, ImplCastExprToType(call_args[3], uint_type, ast::CastKind::IntegralCast));
        }
      }
      break;
    }
    default: {
      UNREACHABLE("Impossible sorter sort call");
    }
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinSorterFree(ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  // First argument must be a pointer to a Sorter
  const auto sorter_kind = ast::BuiltinType::Sorter;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), sorter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_kind)->PointerTo());
    return;
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinSorterIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &args = call->Arguments();

  const auto sorter_iter_kind = ast::BuiltinType::SorterIterator;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), sorter_iter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_iter_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::SorterIterInit: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // The second argument is the sorter instance to iterate over
      const auto sorter_kind = ast::BuiltinType::Sorter;
      if (!IsPointerToSpecificBuiltin(args[1]->GetType(), sorter_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(sorter_kind)->PointerTo());
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::SorterIterHasNext: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::SorterIterNext: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::SorterIterSkipRows: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      const auto uint_kind = ast::BuiltinType::Kind::Uint32;
      if (!args[1]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(uint_kind));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::SorterIterGetRow: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
      break;
    }
    case ast::Builtin::SorterIterClose: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void Sema::CheckBuiltinCall(ast::CallExpr *call) {
  ast::Builtin builtin;
  if (!context_->IsBuiltinFunction(call->GetFuncName(), &builtin)) {
    error_reporter_->Report(call->Function()->Position(), ErrorMessages::kInvalidBuiltinFunction,
                            call->GetFuncName());
    return;
  }

  if (builtin == ast::Builtin::IntCast) {
    CheckBuiltinIntCast(call);
    return;
  }

  if (builtin == ast::Builtin::PtrCast) {
    CheckBuiltinPtrCastCall(call);
    return;
  }

  if (builtin == ast::Builtin::OffsetOf) {
    CheckBuiltinOffsetOfCall(call);
    return;
  }

  // First, resolve all call arguments. If any fail, exit immediately.
  for (auto *arg : call->Arguments()) {
    auto *resolved_type = Resolve(arg);
    if (resolved_type == nullptr) {
      return;
    }
  }

  switch (builtin) {
    case ast::Builtin::BoolToSql:
    case ast::Builtin::IntToSql:
    case ast::Builtin::FloatToSql:
    case ast::Builtin::DateToSql:
    case ast::Builtin::StringToSql:
    case ast::Builtin::SqlToBool:
    case ast::Builtin::ConvertBoolToInteger:
    case ast::Builtin::ConvertIntegerToReal:
    case ast::Builtin::ConvertDateToTimestamp:
    case ast::Builtin::ConvertStringToBool:
    case ast::Builtin::ConvertStringToInt:
    case ast::Builtin::ConvertStringToReal:
    case ast::Builtin::ConvertStringToDate:
    case ast::Builtin::ConvertStringToTime: {
      CheckSqlConversionCall(call, builtin);
      break;
    }
    case ast::Builtin::IsValNull: {
      CheckNullValueCall(call, builtin);
      break;
    }
    case ast::Builtin::Like: {
      CheckBuiltinStringLikeCall(call);
      break;
    }
    case ast::Builtin::ExtractYear: {
      CheckBuiltinDateFunctionCall(call, builtin);
      break;
    }
    case ast::Builtin::Concat: {
      CheckBuiltinConcat(call);
      break;
    }
    case ast::Builtin::ExecutionContextGetMemoryPool:
    case ast::Builtin::ExecutionContextGetTLS: {
      CheckBuiltinExecutionContextCall(call, builtin);
      break;
    }
    case ast::Builtin::ThreadStateContainerReset:
    case ast::Builtin::ThreadStateContainerGetState:
    case ast::Builtin::ThreadStateContainerIterate:
    case ast::Builtin::ThreadStateContainerClear: {
      CheckBuiltinThreadStateContainerCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterInit:
    case ast::Builtin::TableIterAdvance:
    case ast::Builtin::TableIterGetVPI:
    case ast::Builtin::TableIterClose: {
      CheckBuiltinTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterParallel: {
      CheckBuiltinTableIterParCall(call);
      break;
    }
    case ast::Builtin::VPIInit:
    case ast::Builtin::VPIFree:
    case ast::Builtin::VPIIsFiltered:
    case ast::Builtin::VPIGetSelectedRowCount:
    case ast::Builtin::VPIGetVectorProjection:
    case ast::Builtin::VPIHasNext:
    case ast::Builtin::VPIAdvance:
    case ast::Builtin::VPISetPosition:
    case ast::Builtin::VPIMatch:
    case ast::Builtin::VPIReset:
    case ast::Builtin::VPIGetBool:
    case ast::Builtin::VPIGetTinyInt:
    case ast::Builtin::VPIGetSmallInt:
    case ast::Builtin::VPIGetInt:
    case ast::Builtin::VPIGetBigInt:
    case ast::Builtin::VPIGetReal:
    case ast::Builtin::VPIGetDouble:
    case ast::Builtin::VPIGetDate:
    case ast::Builtin::VPIGetString:
    case ast::Builtin::VPIGetPointer:
    case ast::Builtin::VPISetBool:
    case ast::Builtin::VPISetTinyInt:
    case ast::Builtin::VPISetSmallInt:
    case ast::Builtin::VPISetInt:
    case ast::Builtin::VPISetBigInt:
    case ast::Builtin::VPISetReal:
    case ast::Builtin::VPISetDouble:
    case ast::Builtin::VPISetDate:
    case ast::Builtin::VPISetString: {
      CheckBuiltinVPICall(call, builtin);
      break;
    }
    case ast::Builtin::CompactStorageWriteBool:
    case ast::Builtin::CompactStorageWriteTinyInt:
    case ast::Builtin::CompactStorageWriteSmallInt:
    case ast::Builtin::CompactStorageWriteInteger:
    case ast::Builtin::CompactStorageWriteBigInt:
    case ast::Builtin::CompactStorageWriteReal:
    case ast::Builtin::CompactStorageWriteDouble:
    case ast::Builtin::CompactStorageWriteDate:
    case ast::Builtin::CompactStorageWriteTimestamp:
    case ast::Builtin::CompactStorageWriteString: {
      CheckBuiltinCompactStorageWriteCall(call, builtin);
      break;
    }
    case ast::Builtin::CompactStorageReadBool:
    case ast::Builtin::CompactStorageReadTinyInt:
    case ast::Builtin::CompactStorageReadSmallInt:
    case ast::Builtin::CompactStorageReadInteger:
    case ast::Builtin::CompactStorageReadBigInt:
    case ast::Builtin::CompactStorageReadReal:
    case ast::Builtin::CompactStorageReadDouble:
    case ast::Builtin::CompactStorageReadDate:
    case ast::Builtin::CompactStorageReadTimestamp:
    case ast::Builtin::CompactStorageReadString: {
      CheckBuiltinCompactStorageReadCall(call, builtin);
      break;
    }
    case ast::Builtin::Hash: {
      CheckBuiltinHashCall(call, builtin);
      break;
    }
    case ast::Builtin::FilterManagerInit:
    case ast::Builtin::FilterManagerInsertFilter:
    case ast::Builtin::FilterManagerRunFilters:
    case ast::Builtin::FilterManagerFree: {
      CheckBuiltinFilterManagerCall(call, builtin);
      break;
    }
    case ast::Builtin::VectorFilterEqual:
    case ast::Builtin::VectorFilterGreaterThan:
    case ast::Builtin::VectorFilterGreaterThanEqual:
    case ast::Builtin::VectorFilterLessThan:
    case ast::Builtin::VectorFilterLessThanEqual:
    case ast::Builtin::VectorFilterNotEqual: {
      CheckBuiltinVectorFilterCall(call);
      break;
    }
    case ast::Builtin::AggHashTableInit:
    case ast::Builtin::AggHashTableInsert:
    case ast::Builtin::AggHashTableLinkEntry:
    case ast::Builtin::AggHashTableLookup:
    case ast::Builtin::AggHashTableProcessBatch:
    case ast::Builtin::AggHashTableMovePartitions:
    case ast::Builtin::AggHashTableParallelPartitionedScan:
    case ast::Builtin::AggHashTableFree: {
      CheckBuiltinAggHashTableCall(call, builtin);
      break;
    }
    case ast::Builtin::AggHashTableIterInit:
    case ast::Builtin::AggHashTableIterHasNext:
    case ast::Builtin::AggHashTableIterNext:
    case ast::Builtin::AggHashTableIterGetRow:
    case ast::Builtin::AggHashTableIterClose: {
      CheckBuiltinAggHashTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::AggPartIterHasNext:
    case ast::Builtin::AggPartIterNext:
    case ast::Builtin::AggPartIterGetRow:
    case ast::Builtin::AggPartIterGetRowEntry:
    case ast::Builtin::AggPartIterGetHash: {
      CheckBuiltinAggPartIterCall(call, builtin);
      break;
    }
    case ast::Builtin::AggInit:
    case ast::Builtin::AggAdvance:
    case ast::Builtin::AggMerge:
    case ast::Builtin::AggReset:
    case ast::Builtin::AggResult: {
      CheckBuiltinAggregatorCall(call, builtin);
      break;
    }
    case ast::Builtin::JoinHashTableInit:
    case ast::Builtin::JoinHashTableInsert:
    case ast::Builtin::JoinHashTableBuild:
    case ast::Builtin::JoinHashTableBuildParallel:
    case ast::Builtin::JoinHashTableLookup:
    case ast::Builtin::JoinHashTableFree: {
      CheckBuiltinJoinHashTableCall(call, builtin);
      break;
    }
    case ast::Builtin::HashTableEntryGetHash:
    case ast::Builtin::HashTableEntryGetRow:
    case ast::Builtin::HashTableEntryGetNext: {
      CheckBuiltinHashTableEntryCall(call, builtin);
      break;
    }
    case ast::Builtin::SorterInit: {
      CheckBuiltinSorterInit(call);
      break;
    }
    case ast::Builtin::SorterInsert:
    case ast::Builtin::SorterInsertTopK:
    case ast::Builtin::SorterInsertTopKFinish: {
      CheckBuiltinSorterInsert(call, builtin);
      break;
    }
    case ast::Builtin::SorterSort:
    case ast::Builtin::SorterSortParallel:
    case ast::Builtin::SorterSortTopKParallel: {
      CheckBuiltinSorterSort(call, builtin);
      break;
    }
    case ast::Builtin::SorterFree: {
      CheckBuiltinSorterFree(call);
      break;
    }
    case ast::Builtin::SorterIterInit:
    case ast::Builtin::SorterIterHasNext:
    case ast::Builtin::SorterIterNext:
    case ast::Builtin::SorterIterSkipRows:
    case ast::Builtin::SorterIterGetRow:
    case ast::Builtin::SorterIterClose: {
      CheckBuiltinSorterIterCall(call, builtin);
      break;
    }
    case ast::Builtin::ResultBufferAllocOutRow:
    case ast::Builtin::ResultBufferFinalize: {
      CheckResultBufferCall(call, builtin);
      break;
    }
    case ast::Builtin::CSVReaderInit:
    case ast::Builtin::CSVReaderAdvance:
    case ast::Builtin::CSVReaderGetField:
    case ast::Builtin::CSVReaderGetRecordNumber:
    case ast::Builtin::CSVReaderClose: {
      CheckCSVReaderCall(call, builtin);
      break;
    }
    case ast::Builtin::ACos:
    case ast::Builtin::ASin:
    case ast::Builtin::ATan:
    case ast::Builtin::ATan2:
    case ast::Builtin::Cos:
    case ast::Builtin::Cot:
    case ast::Builtin::Sin:
    case ast::Builtin::Tan: {
      CheckMathTrigCall(call, builtin);
      break;
    }
    case ast::Builtin::SizeOf: {
      CheckBuiltinSizeOfCall(call);
      break;
    }
    case ast::Builtin::OffsetOf:
    case ast::Builtin::IntCast:
    case ast::Builtin::PtrCast: {
      UNREACHABLE("These operations should be handled PRIOR to switch.");
    }
  }
}

}  // namespace tpl::sema
