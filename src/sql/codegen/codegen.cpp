#include "sql/codegen/codegen.h"

#include "spdlog/fmt/fmt.h"

#include "ast/ast_node_factory.h"
#include "ast/builtins.h"
#include "ast/context.h"
#include "ast/type.h"
#include "ast/type_visitor.h"
#include "common/exception.h"

namespace tpl::sql::codegen {

//===----------------------------------------------------------------------===//
//
// Scopes
//
//===----------------------------------------------------------------------===//

std::string CodeGen::LexicalScope::GetFreshName(std::string_view name) {
  // Attempt insert.
  auto insert_result = names_.insert(std::make_pair(name, 1));
  if (insert_result.second) {
    return insert_result.first->getKey().str();
  }
  // Duplicate found. Find a new version that hasn't already been declared.
  uint64_t &id = insert_result.first->getValue();
  while (true) {
    const std::string next_name = fmt::format("{}{}", name, id++);
    if (names_.find(next_name) == names_.end()) {
      return next_name;
    }
  }
}

//===----------------------------------------------------------------------===//
//
// Code Generator
//
//===----------------------------------------------------------------------===//

CodeGen::CodeGen(CompilationUnit *container)
    : container_(container),
      position_{0, 0},
      num_cached_scopes_(0),
      scope_(nullptr),
      function_(nullptr) {
  // Create the scopes.
  for (uint32_t i = 0; i < scope_cache_.max_size(); i++) {
    scope_cache_[i] = std::make_unique<LexicalScope>(nullptr);
  }
  num_cached_scopes_ = kDefaultScopeCacheSize;
  EnterScope();
}

CodeGen::~CodeGen() { ExitScope(); }

ast::Expression *CodeGen::ConstBool(bool val) const {
  ast::Expression *expr = NodeFactory()->NewBoolLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return expr;
}

ast::Expression *CodeGen::Const8(int8_t val) const {
  ast::Expression *expr = NodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Int8));
  return expr;
}

ast::Expression *CodeGen::Const16(int16_t val) const {
  ast::Expression *expr = NodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Int16));
  return expr;
}

ast::Expression *CodeGen::Const32(int32_t val) const {
  ast::Expression *expr = NodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Int32));
  return expr;
}

ast::Expression *CodeGen::Const64(int64_t val) const {
  ast::Expression *expr = NodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Int64));
  return expr;
}

ast::Expression *CodeGen::ConstDouble(double val) const {
  ast::Expression *expr = NodeFactory()->NewFloatLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Float32));
  return expr;
}

ast::Expression *CodeGen::ConstString(std::string_view str) const {
  ast::Expression *expr = NodeFactory()->NewStringLiteral(position_, MakeIdentifier(str));
  expr->SetType(ast::StringType::Get(Context()));
  return expr;
}

ast::VariableDeclaration *CodeGen::DeclareVar(ast::Identifier name, ast::Expression *type_repr,
                                              ast::Expression *init) {
  return NodeFactory()->NewVariableDeclaration(position_, name, type_repr, init);
}

ast::VariableDeclaration *CodeGen::DeclareVarNoInit(ast::Identifier name,
                                                    ast::Expression *type_repr) {
  return DeclareVar(name, type_repr, nullptr);
}

ast::VariableDeclaration *CodeGen::DeclareVarNoInit(ast::Identifier name,
                                                    ast::BuiltinType::Kind kind) {
  return DeclareVarNoInit(name, BuiltinType(kind));
}

ast::VariableDeclaration *CodeGen::DeclareVarWithInit(ast::Identifier name, ast::Expression *init) {
  return DeclareVar(name, nullptr, init);
}

ast::StructDeclaration *CodeGen::DeclareStruct(
    ast::Identifier name, util::RegionVector<ast::FieldDeclaration *> &&fields) const {
  auto type_repr = NodeFactory()->NewStructType(position_, std::move(fields));
  auto decl = NodeFactory()->NewStructDeclaration(position_, name, type_repr);
  container_->RegisterStruct(decl);
  return decl;
}

ast::Statement *CodeGen::Assign(ast::Expression *dest, ast::Expression *value) {
  TPL_ASSERT(dest->GetType() == value->GetType(), "Mismatched types!");
  return NodeFactory()->NewAssignmentStatement(position_, dest, value);
}

ast::Expression *CodeGen::ArrayType(uint64_t num_elems, ast::Expression *type_repr) {
  ast::Expression *length = num_elems == 0 ? nullptr : Const64(num_elems);
  return NodeFactory()->NewArrayType(position_, length, type_repr);
}

ast::Expression *CodeGen::ArrayType(uint64_t num_elems, ast::BuiltinType::Kind kind) {
  return ArrayType(num_elems, BuiltinType(kind));
}

ast::Expression *CodeGen::ArrayAccess(ast::Expression *arr, ast::Expression *idx) {
  ast::Expression *result = NodeFactory()->NewIndexExpression(position_, arr, idx);
  result->SetType(arr->GetType()->As<ast::ArrayType>()->GetElementType());
  return result;
}

ast::Expression *CodeGen::ArrayAccess(ast::Identifier arr, uint64_t idx) {
  return ArrayAccess(MakeExpr(arr), Const64(idx));
}

ast::Expression *CodeGen::BuiltinType(ast::BuiltinType::Kind builtin_kind) const {
  // Lookup the builtin type. We'll use it to construct an identifier.
  ast::BuiltinType *type = ast::BuiltinType::Get(Context(), builtin_kind);
  // Build an identifier expression using the builtin's name
  ast::Expression *expr = MakeExpr(Context()->GetIdentifier(type->GetTplName()));
  // Set the type to avoid double-checking the type
  expr->SetType(type);
  // Done
  return expr;
}

ast::Expression *CodeGen::BoolType() const { return BuiltinType(ast::BuiltinType::Bool); }

ast::Expression *CodeGen::Int8Type() const { return BuiltinType(ast::BuiltinType::Int8); }

ast::Expression *CodeGen::Int16Type() const { return BuiltinType(ast::BuiltinType::Int16); }

ast::Expression *CodeGen::Int32Type() const { return BuiltinType(ast::BuiltinType::Int32); }

ast::Expression *CodeGen::UInt32Type() const { return BuiltinType(ast::BuiltinType::UInt32); }

ast::Expression *CodeGen::Int64Type() const { return BuiltinType(ast::BuiltinType::Int64); }

ast::Expression *CodeGen::Float32Type() const { return BuiltinType(ast::BuiltinType::Float32); }

ast::Expression *CodeGen::Float64Type() const { return BuiltinType(ast::BuiltinType::Float64); }

ast::Expression *CodeGen::PointerType(ast::Expression *base_type_repr) const {
  // Create the type representation
  auto *type_repr = NodeFactory()->NewPointerType(position_, base_type_repr);
  // Set the actual TPL type
  if (base_type_repr->GetType() != nullptr) {
    type_repr->SetType(ast::PointerType::Get(base_type_repr->GetType()));
  }
  // Done
  return type_repr;
}

ast::Expression *CodeGen::PointerType(ast::Identifier type_name) const {
  return PointerType(MakeExpr(type_name));
}

ast::Expression *CodeGen::PointerType(ast::BuiltinType::Kind builtin) const {
  return PointerType(BuiltinType(builtin));
}

ast::Expression *CodeGen::TplType(sql::TypeId type) {
  switch (type) {
    case sql::TypeId::Boolean:
      return BuiltinType(ast::BuiltinType::BooleanVal);
    case sql::TypeId::TinyInt:
    case sql::TypeId::SmallInt:
    case sql::TypeId::Integer:
    case sql::TypeId::BigInt:
      return BuiltinType(ast::BuiltinType::IntegerVal);
    case sql::TypeId::Date:
      return BuiltinType(ast::BuiltinType::DateVal);
    case sql::TypeId::Timestamp:
      return BuiltinType(ast::BuiltinType::TimestampVal);
    case sql::TypeId::Double:
    case sql::TypeId::Float:
      return BuiltinType(ast::BuiltinType::RealVal);
    case sql::TypeId::Varchar:
      return BuiltinType(ast::BuiltinType::StringVal);
    default:
      UNREACHABLE("Cannot codegen unsupported type.");
  }
}

ast::Expression *CodeGen::PrimitiveTplType(TypeId type) {
  switch (type) {
    case sql::TypeId::Boolean:
      return BuiltinType(ast::BuiltinType::Bool);
    case sql::TypeId::TinyInt:
      return BuiltinType(ast::BuiltinType::Int8);
    case sql::TypeId::SmallInt:
      return BuiltinType(ast::BuiltinType::Int16);
    case sql::TypeId::Integer:
      return BuiltinType(ast::BuiltinType::Int32);
    case sql::TypeId::BigInt:
      return BuiltinType(ast::BuiltinType::Int64);
    case sql::TypeId::Float:
      return BuiltinType(ast::BuiltinType::Float32);
    case sql::TypeId::Double:
      return BuiltinType(ast::BuiltinType::Float64);
    case sql::TypeId::Date:
      return BuiltinType(ast::BuiltinType::Date);
    case sql::TypeId::Timestamp:
      return BuiltinType(ast::BuiltinType::Timestamp);
    case sql::TypeId::Varchar:
      return BuiltinType(ast::BuiltinType::VarlenEntry);
    default:
      UNREACHABLE("Cannot codegen unsupported type.");
  }
}

ast::Expression *CodeGen::AggregateType(planner::ExpressionType agg_type, TypeId ret_type) const {
  switch (agg_type) {
    case planner::ExpressionType::AGGREGATE_COUNT:
      return BuiltinType(ast::BuiltinType::Kind::CountAggregate);
    case planner::ExpressionType::AGGREGATE_AVG:
      return BuiltinType(ast::BuiltinType::AvgAggregate);
    case planner::ExpressionType::AGGREGATE_MIN:
      if (IsTypeIntegral(ret_type)) {
        return BuiltinType(ast::BuiltinType::IntegerMinAggregate);
      } else if (IsTypeFloatingPoint(ret_type)) {
        return BuiltinType(ast::BuiltinType::RealMinAggregate);
      } else if (ret_type == TypeId::Date) {
        return BuiltinType(ast::BuiltinType::DateMinAggregate);
      } else if (ret_type == TypeId::Varchar) {
        return BuiltinType(ast::BuiltinType::StringMinAggregate);
      } else {
        throw NotImplementedException(
            fmt::format("MIN() aggregates on type {}", TypeIdToString(ret_type)));
      }
    case planner::ExpressionType::AGGREGATE_MAX:
      if (IsTypeIntegral(ret_type)) {
        return BuiltinType(ast::BuiltinType::IntegerMaxAggregate);
      } else if (IsTypeFloatingPoint(ret_type)) {
        return BuiltinType(ast::BuiltinType::RealMaxAggregate);
      } else if (ret_type == TypeId::Date) {
        return BuiltinType(ast::BuiltinType::DateMaxAggregate);
      } else if (ret_type == TypeId::Varchar) {
        return BuiltinType(ast::BuiltinType::StringMaxAggregate);
      } else {
        throw NotImplementedException(
            fmt::format("MAX() aggregates on type {}", TypeIdToString(ret_type)));
      }
    case planner::ExpressionType::AGGREGATE_SUM:
      TPL_ASSERT(IsTypeNumeric(ret_type), "Only arithmetic types have sums.");
      if (IsTypeIntegral(ret_type)) {
        return BuiltinType(ast::BuiltinType::IntegerSumAggregate);
      }
      return BuiltinType(ast::BuiltinType::RealSumAggregate);
    default: {
      UNREACHABLE("AggregateType() should only be called with aggregates.");
    }
  }
}

ast::Expression *CodeGen::Nil() const {
  ast::Expression *expr = NodeFactory()->NewNilLiteral(position_);
  expr->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return expr;
}

ast::Expression *CodeGen::AddressOf(ast::Expression *obj) const {
  return UnaryOp(parsing::Token::Type::AMPERSAND, obj);
}

ast::Expression *CodeGen::AddressOf(ast::Identifier obj_name) const {
  return UnaryOp(parsing::Token::Type::AMPERSAND, MakeExpr(obj_name));
}

ast::Expression *CodeGen::SizeOf(ast::Identifier type_name) const {
  return CallBuiltin(ast::Builtin::SizeOf, {MakeExpr(type_name)});
}

ast::Expression *CodeGen::OffsetOf(ast::Identifier obj, ast::Identifier member) const {
  return CallBuiltin(ast::Builtin::OffsetOf, {MakeExpr(obj), MakeExpr(member)});
}

ast::Expression *CodeGen::PtrCast(ast::Expression *base, ast::Expression *arg) const {
  ast::Expression *ptr =
      NodeFactory()->NewUnaryOpExpression(position_, parsing::Token::Type::STAR, base);
  return CallBuiltin(ast::Builtin::PtrCast, {ptr, arg});
}

ast::Expression *CodeGen::PtrCast(ast::Identifier base_name, ast::Expression *arg) const {
  return PtrCast(MakeExpr(base_name), arg);
}

ast::Expression *CodeGen::BinaryOp(parsing::Token::Type op, ast::Expression *left,
                                   ast::Expression *right) const {
  TPL_ASSERT(parsing::Token::IsBinaryOp(op), "Provided operation isn't binary");
  return NodeFactory()->NewBinaryOpExpression(position_, op, left, right);
}

ast::Expression *CodeGen::Compare(parsing::Token::Type op, ast::Expression *left,
                                  ast::Expression *right) const {
  return NodeFactory()->NewComparisonOpExpression(position_, op, left, right);
}

ast::Expression *CodeGen::IsNilPointer(ast::Expression *obj) const {
  return Compare(parsing::Token::Type::EQUAL_EQUAL, obj, Nil());
}

ast::Expression *CodeGen::UnaryOp(parsing::Token::Type op, ast::Expression *input) const {
  return NodeFactory()->NewUnaryOpExpression(position_, op, input);
}

// ---------------------------------------------------------
// Bit Operations.
// ---------------------------------------------------------

ast::Expression *CodeGen::BitAnd(ast::Expression *lhs, ast::Expression *rhs) const {
  return BinaryOp(parsing::Token::Type::AMPERSAND, lhs, rhs);
}

ast::Expression *CodeGen::BitOr(ast::Expression *lhs, ast::Expression *rhs) const {
  return BinaryOp(parsing::Token::Type::BIT_OR, lhs, rhs);
}

ast::Expression *CodeGen::BitShiftLeft(ast::Expression *val, ast::Expression *num_bits) const {
  return BinaryOp(parsing::Token::Type::BIT_SHL, val, num_bits);
}

ast::Expression *CodeGen::BitShiftRight(ast::Expression *val, ast::Expression *num_bits) const {
  return BinaryOp(parsing::Token::Type::BIT_SHR, val, num_bits);
}

ast::Expression *CodeGen::Add(ast::Expression *left, ast::Expression *right) const {
  return BinaryOp(parsing::Token::Type::PLUS, left, right);
}

ast::Expression *CodeGen::Sub(ast::Expression *left, ast::Expression *right) const {
  return BinaryOp(parsing::Token::Type::MINUS, left, right);
}

ast::Expression *CodeGen::Mul(ast::Expression *left, ast::Expression *right) const {
  return BinaryOp(parsing::Token::Type::STAR, left, right);
}

ast::Expression *CodeGen::AccessStructMember(ast::Expression *object, ast::Identifier member) {
  return NodeFactory()->NewMemberExpression(position_, object, MakeExpr(member));
}

ast::Statement *CodeGen::Return() { return Return(nullptr); }

ast::Statement *CodeGen::Return(ast::Expression *ret) {
  ast::Statement *stmt = NodeFactory()->NewReturnStatement(position_, ret);
  NewLine();
  return stmt;
}

ast::Expression *CodeGen::Call(ast::Identifier func_name,
                               std::initializer_list<ast::Expression *> args) const {
  util::RegionVector<ast::Expression *> call_args(args, Context()->GetRegion());
  return NodeFactory()->NewCallExpression(MakeExpr(func_name), std::move(call_args));
}

ast::Expression *CodeGen::Call(ast::Identifier func_name,
                               const std::vector<ast::Expression *> &args) const {
  util::RegionVector<ast::Expression *> call_args(args.begin(), args.end(), Context()->GetRegion());
  return NodeFactory()->NewCallExpression(MakeExpr(func_name), std::move(call_args));
}

ast::Expression *CodeGen::CallBuiltin(ast::Builtin builtin,
                                      std::initializer_list<ast::Expression *> args) const {
  util::RegionVector<ast::Expression *> call_args(args, Context()->GetRegion());
  ast::Expression *func =
      MakeExpr(Context()->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
  ast::Expression *call = NodeFactory()->NewBuiltinCallExpression(func, std::move(call_args));
  return call;
}

// This is copied from the overloaded function. But, we use initializer so often we keep it around.
ast::Expression *CodeGen::CallBuiltin(ast::Builtin builtin,
                                      const std::vector<ast::Expression *> &args) const {
  util::RegionVector<ast::Expression *> call_args(args.begin(), args.end(), Context()->GetRegion());
  ast::Expression *func =
      MakeExpr(Context()->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
  ast::Expression *call = NodeFactory()->NewBuiltinCallExpression(func, std::move(call_args));
  return call;
}

ast::Expression *CodeGen::BoolToSql(bool b) const {
  ast::Expression *call = CallBuiltin(ast::Builtin::BoolToSql, {ConstBool(b)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::BooleanVal));
  return call;
}

ast::Expression *CodeGen::IntToSql(int64_t num) const {
  ast::Expression *call = CallBuiltin(ast::Builtin::IntToSql, {Const64(num)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::IntegerVal));
  return call;
}

ast::Expression *CodeGen::FloatToSql(double num) const {
  ast::Expression *call = CallBuiltin(ast::Builtin::FloatToSql, {ConstDouble(num)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::RealVal));
  return call;
}

ast::Expression *CodeGen::DateToSql(Date date) const {
  int32_t year, month, day;
  date.ExtractComponents(&year, &month, &day);
  return DateToSql(year, month, day);
}

ast::Expression *CodeGen::DateToSql(int32_t year, int32_t month, int32_t day) const {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::DateToSql, {Const32(year), Const32(month), Const32(day)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::DateVal));
  return call;
}

ast::Expression *CodeGen::StringToSql(std::string_view str) const {
  ast::Expression *call = CallBuiltin(ast::Builtin::StringToSql, {ConstString(str)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::StringVal));
  return call;
}

ast::Expression *CodeGen::ConvertSql(ast::Expression *input, sql::TypeId from_type,
                                     sql::TypeId to_type) const {
  if (from_type == to_type) {
    return input;
  }

  // TODO(pmenon): The compiler will convert the below switch into a lookup
  //               table for us, but we should do it manually for readability.
  switch (from_type) {
    case TypeId::Boolean:
      switch (to_type) {
        case TypeId::TinyInt:
        case TypeId::SmallInt:
        case TypeId::Integer:
        case TypeId::BigInt:
          return CallBuiltin(ast::Builtin::ConvertBoolToInteger, {input});
        default:
          break;
      }
      break;
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
      switch (to_type) {
        case TypeId::Float:
        case TypeId::Double:
          return CallBuiltin(ast::Builtin::ConvertIntegerToReal, {input});
        default:
          break;
      }
      break;
    case TypeId::Date:
      switch (to_type) {
        case TypeId::Timestamp:
          return CallBuiltin(ast::Builtin::ConvertDateToTimestamp, {input});
        default:
          break;
      }
      break;
    case TypeId::Varchar:
      switch (to_type) {
        case TypeId::Boolean:
          return CallBuiltin(ast::Builtin::ConvertStringToBool, {input});
        case TypeId::BigInt:
          return CallBuiltin(ast::Builtin::ConvertStringToInt, {input});
        case TypeId::Double:
          return CallBuiltin(ast::Builtin::ConvertStringToReal, {input});
        case TypeId::Date:
          return CallBuiltin(ast::Builtin::ConvertStringToDate, {input});
        case TypeId::Timestamp:
          return CallBuiltin(ast::Builtin::ConvertStringToTime, {input});
        default:
          break;
      }
      break;
    default:
      break;
  }

  throw ConversionException(fmt::format("Cannot convert from SQL type '{}' to type '{}'.",
                                        TypeIdToString(from_type), TypeIdToString(to_type)));
}

ast::Expression *CodeGen::InitSqlNull(ast::Expression *val) const {
  return CallBuiltin(ast::Builtin::InitSqlNull, {AddressOf(val)});
}

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

ast::Expression *CodeGen::TableIterInit(ast::Expression *table_iter, std::string_view table_name) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::TableIterInit, {table_iter, ConstString(table_name)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::TableIterAdvance(ast::Expression *table_iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::TableIterAdvance, {table_iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return call;
}

ast::Expression *CodeGen::TableIterGetVPI(ast::Expression *table_iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::TableIterGetVPI, {table_iter});
  call->SetType(
      ast::BuiltinType::Get(Context(), ast::BuiltinType::VectorProjectionIterator)->PointerTo());
  return call;
}

ast::Expression *CodeGen::TableIterClose(ast::Expression *table_iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::TableIterClose, {table_iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::IterateTableParallel(std::string_view table_name,
                                               ast::Expression *query_state, ast::Expression *tls,
                                               ast::Identifier worker_name) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::TableIterParallel,
                  {ConstString(table_name), query_state, tls, MakeExpr(worker_name)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Vector Projection Iterator
// ---------------------------------------------------------

ast::Expression *CodeGen::VPIIsFiltered(ast::Expression *vpi) {
  ast::Expression *call = CallBuiltin(ast::Builtin::VPIIsFiltered, {vpi});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return call;
}

ast::Expression *CodeGen::VPIHasNext(ast::Expression *vpi) {
  ast::Expression *call = CallBuiltin(ast::Builtin::VPIHasNext, {vpi});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return call;
}

ast::Expression *CodeGen::VPIAdvance(ast::Expression *vpi) {
  ast::Expression *call = CallBuiltin(ast::Builtin::VPIAdvance, {vpi});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::VPIReset(ast::Expression *vpi) {
  ast::Expression *call = CallBuiltin(ast::Builtin::VPIReset, {vpi});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::VPIMatch(ast::Expression *vpi, ast::Expression *cond) {
  ast::Expression *call = CallBuiltin(ast::Builtin::VPIMatch, {vpi, cond});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::VPIInit(ast::Expression *vpi, ast::Expression *vp,
                                  ast::Expression *tids) {
  ast::Expression *call = nullptr;
  if (tids != nullptr) {
    call = CallBuiltin(ast::Builtin::VPIInit, {vpi, vp, tids});
  } else {
    call = CallBuiltin(ast::Builtin::VPIInit, {vpi, vp});
  }
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::VPIGet(ast::Expression *vpi, sql::TypeId type_id, bool nullable,
                                 uint32_t idx) {
  ast::Builtin builtin;
  ast::BuiltinType::Kind ret_kind;
  switch (type_id) {
    case sql::TypeId::Boolean:
      builtin = ast::Builtin::VPIGetBool;
      ret_kind = ast::BuiltinType::BooleanVal;
      break;
    case sql::TypeId::TinyInt:
      builtin = ast::Builtin::VPIGetTinyInt;
      ret_kind = ast::BuiltinType::IntegerVal;
      break;
    case sql::TypeId::SmallInt:
      builtin = ast::Builtin::VPIGetSmallInt;
      ret_kind = ast::BuiltinType::IntegerVal;
      break;
    case sql::TypeId::Integer:
      builtin = ast::Builtin::VPIGetInt;
      ret_kind = ast::BuiltinType::IntegerVal;
      break;
    case sql::TypeId::BigInt:
      builtin = ast::Builtin::VPIGetBigInt;
      ret_kind = ast::BuiltinType::IntegerVal;
      break;
    case sql::TypeId::Float:
      builtin = ast::Builtin::VPIGetReal;
      ret_kind = ast::BuiltinType::RealVal;
      break;
    case sql::TypeId::Double:
      builtin = ast::Builtin::VPIGetDouble;
      ret_kind = ast::BuiltinType::RealVal;
      break;
    case sql::TypeId::Date:
      builtin = ast::Builtin::VPIGetDate;
      ret_kind = ast::BuiltinType::DateVal;
      break;
    case sql::TypeId::Varchar:
      builtin = ast::Builtin::VPIGetString;
      ret_kind = ast::BuiltinType::StringVal;
      break;
    default:
      throw NotImplementedException(
          fmt::format("CodeGen: Reading type {} from VPI not supported.", TypeIdToString(type_id)));
  }
  ast::Expression *call = CallBuiltin(builtin, {vpi, Const32(idx)});
  call->SetType(ast::BuiltinType::Get(Context(), ret_kind));
  return call;
}

ast::Expression *CodeGen::VPIFilter(ast::Expression *vp, planner::ExpressionType comp_type,
                                    uint32_t col_idx, ast::Expression *filter_val,
                                    ast::Expression *tids) {
  // Call @FilterComp(vpi, col_idx, col_type, filter_val)
  ast::Builtin builtin;
  switch (comp_type) {
    case planner::ExpressionType::COMPARE_EQUAL:
      builtin = ast::Builtin::VectorFilterEqual;
      break;
    case planner::ExpressionType::COMPARE_NOT_EQUAL:
      builtin = ast::Builtin::VectorFilterNotEqual;
      break;
    case planner::ExpressionType::COMPARE_LESS_THAN:
      builtin = ast::Builtin::VectorFilterLessThan;
      break;
    case planner::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::VectorFilterLessThanEqual;
      break;
    case planner::ExpressionType::COMPARE_GREATER_THAN:
      builtin = ast::Builtin::VectorFilterGreaterThan;
      break;
    case planner::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::VectorFilterGreaterThanEqual;
      break;
    default:
      throw NotImplementedException(
          fmt::format("CodeGen: Vector filter type {} from VPI not supported.",
                      planner::ExpressionTypeToString(comp_type, true)));
  }
  ast::Expression *call = CallBuiltin(builtin, {vp, Const32(col_idx), filter_val, tids});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

ast::Expression *CodeGen::FilterManagerInit(ast::Expression *filter_manager) {
  ast::Expression *call = CallBuiltin(ast::Builtin::FilterManagerInit, {filter_manager});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::FilterManagerFree(ast::Expression *filter_manager) {
  ast::Expression *call = CallBuiltin(ast::Builtin::FilterManagerFree, {filter_manager});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::FilterManagerInsert(ast::Expression *filter_manager,
                                              const std::vector<ast::Identifier> &clause_fn_names) {
  std::vector<ast::Expression *> params(1 + clause_fn_names.size());
  params[0] = filter_manager;
  for (uint32_t i = 0; i < clause_fn_names.size(); i++) {
    params[i + 1] = MakeExpr(clause_fn_names[i]);
  }
  ast::Expression *call = CallBuiltin(ast::Builtin::FilterManagerInsertFilter, params);
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::FilterManagerRunFilters(ast::Expression *filter_manager,
                                                  ast::Expression *vpi) {
  ast::Expression *call = CallBuiltin(ast::Builtin::FilterManagerRunFilters, {filter_manager, vpi});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::ExecCtxGetMemoryPool(ast::Expression *exec_ctx) {
  ast::Expression *call = CallBuiltin(ast::Builtin::ExecutionContextGetMemoryPool, {exec_ctx});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::MemoryPool)->PointerTo());
  return call;
}

ast::Expression *CodeGen::ExecCtxGetTLS(ast::Expression *exec_ctx) {
  ast::Expression *call = CallBuiltin(ast::Builtin::ExecutionContextGetTLS, {exec_ctx});
  call->SetType(
      ast::BuiltinType::Get(Context(), ast::BuiltinType::ThreadStateContainer)->PointerTo());
  return call;
}

ast::Expression *CodeGen::TLSAccessCurrentThreadState(ast::Expression *tls,
                                                      ast::Identifier state_type_name) {
  ast::Expression *call = CallBuiltin(ast::Builtin::ThreadStateContainerGetState, {tls});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt8)->PointerTo());
  return PtrCast(state_type_name, call);
}

ast::Expression *CodeGen::TLSIterate(ast::Expression *tls, ast::Expression *context,
                                     ast::Identifier func) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::ThreadStateContainerIterate, {tls, context, MakeExpr(func)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::TLSReset(ast::Expression *tls, ast::Identifier tls_state_name,
                                   ast::Identifier init_fn, ast::Identifier tear_down_fn,
                                   ast::Expression *context) {
  ast::Expression *call = CallBuiltin(
      ast::Builtin::ThreadStateContainerReset,
      {tls, SizeOf(tls_state_name), MakeExpr(init_fn), MakeExpr(tear_down_fn), context});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::TLSClear(ast::Expression *tls) {
  ast::Expression *call = CallBuiltin(ast::Builtin::ThreadStateContainerClear, {tls});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Hash
// ---------------------------------------------------------

ast::Expression *CodeGen::Hash(const std::vector<ast::Expression *> &values) {
  ast::Expression *call = CallBuiltin(ast::Builtin::Hash, values);
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt64));
  return call;
}

// ---------------------------------------------------------
// Joins
// ---------------------------------------------------------

ast::Expression *CodeGen::JoinHashTableInit(ast::Expression *join_hash_table,
                                            ast::Expression *mem_pool,
                                            ast::Identifier build_row_type_name) {
  ast::Expression *call = CallBuiltin(ast::Builtin::JoinHashTableInit,
                                      {join_hash_table, mem_pool, SizeOf(build_row_type_name)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::JoinHashTableInit(ast::Expression *join_hash_table,
                                            ast::Expression *mem_pool,
                                            ast::Identifier build_row_type_name,
                                            ast::Identifier analysis_fn,
                                            ast::Identifier compress_fn) {
  ast::Expression *call = CallBuiltin(ast::Builtin::JoinHashTableInit,
                                      {join_hash_table, mem_pool, SizeOf(build_row_type_name),
                                       MakeExpr(analysis_fn), MakeExpr(compress_fn)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::JoinHashTableInsert(ast::Expression *join_hash_table,
                                              ast::Expression *hash_val,
                                              ast::Identifier tuple_type_name) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::JoinHashTableInsert, {join_hash_table, hash_val});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return PtrCast(tuple_type_name, call);
}

ast::Expression *CodeGen::JoinHashTableBuild(ast::Expression *join_hash_table) {
  ast::Expression *call = CallBuiltin(ast::Builtin::JoinHashTableBuild, {join_hash_table});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::JoinHashTableBuildParallel(ast::Expression *join_hash_table,
                                                     ast::Expression *thread_state_container,
                                                     ast::Expression *offset) {
  ast::Expression *call = CallBuiltin(ast::Builtin::JoinHashTableBuildParallel,
                                      {join_hash_table, thread_state_container, offset});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::JoinHashTableLookup(ast::Expression *join_hash_table,
                                              ast::Expression *hash_val) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::JoinHashTableLookup, {join_hash_table, hash_val});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::JoinHashTableFree(ast::Expression *join_hash_table) {
  ast::Expression *call = CallBuiltin(ast::Builtin::JoinHashTableFree, {join_hash_table});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::HTEntryGetHash(ast::Expression *entry) {
  ast::Expression *call = CallBuiltin(ast::Builtin::HashTableEntryGetHash, {entry});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt64));
  return call;
}

ast::Expression *CodeGen::HTEntryGetRow(ast::Expression *entry, ast::Identifier row_type) {
  ast::Expression *call = CallBuiltin(ast::Builtin::HashTableEntryGetRow, {entry});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt8)->PointerTo());
  return PtrCast(row_type, call);
}

ast::Expression *CodeGen::HTEntryGetNext(ast::Expression *entry) {
  ast::Expression *call = CallBuiltin(ast::Builtin::HashTableEntryGetNext, {entry});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::HashTableEntry)->PointerTo());
  return call;
}

ast::Expression *CodeGen::StatsSetColumnCount(ast::Expression *stats, uint32_t column_count) {
  return CallBuiltin(ast::Builtin::AnalysisStatsSetColumnCount, {stats, Const32(column_count)});
}

ast::Expression *CodeGen::StatsSetColumnBits(ast::Expression *stats, uint32_t column,
                                             ast::Expression *bits) {
  return CallBuiltin(ast::Builtin::AnalysisStatsSetColumnBits, {stats, Const32(column), bits});
}

// ---------------------------------------------------------
// Hash aggregations
// ---------------------------------------------------------

ast::Expression *CodeGen::AggHashTableInit(ast::Expression *agg_ht, ast::Expression *mem_pool,
                                           ast::Identifier agg_payload_type) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::AggHashTableInit, {agg_ht, mem_pool, SizeOf(agg_payload_type)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::AggHashTableLookup(ast::Expression *agg_ht, ast::Expression *hash_val,
                                             ast::Identifier key_check, ast::Expression *input,
                                             ast::Identifier agg_payload_type) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::AggHashTableLookup, {agg_ht, hash_val, MakeExpr(key_check), input});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt8)->PointerTo());
  return PtrCast(agg_payload_type, call);
}

ast::Expression *CodeGen::AggHashTableInsert(ast::Expression *agg_ht, ast::Expression *hash_val,
                                             bool partitioned, ast::Identifier agg_payload_type) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::AggHashTableInsert, {agg_ht, hash_val, ConstBool(partitioned)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt8)->PointerTo());
  return PtrCast(agg_payload_type, call);
}

ast::Expression *CodeGen::AggHashTableLinkEntry(ast::Expression *agg_ht, ast::Expression *entry) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggHashTableLinkEntry, {agg_ht, entry});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::AggHashTableMovePartitions(ast::Expression *agg_ht, ast::Expression *tls,
                                                     ast::Expression *tl_agg_ht_offset,
                                                     ast::Identifier merge_partitions_fn_name) {
  std::initializer_list<ast::Expression *> args = {agg_ht, tls, tl_agg_ht_offset,
                                                   MakeExpr(merge_partitions_fn_name)};
  ast::Expression *call = CallBuiltin(ast::Builtin::AggHashTableMovePartitions, args);
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::AggHashTableParallelScan(ast::Expression *agg_ht,
                                                   ast::Expression *query_state,
                                                   ast::Expression *thread_state_container,
                                                   ast::Identifier worker_fn) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::AggHashTableParallelPartitionedScan,
                  {agg_ht, query_state, thread_state_container, MakeExpr(worker_fn)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::AggHashTableFree(ast::Expression *agg_ht) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggHashTableFree, {agg_ht});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Aggregation Hash Table Overflow Iterator
// ---------------------------------------------------------

ast::Expression *CodeGen::AggPartitionIteratorHasNext(ast::Expression *iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggPartIterHasNext, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return call;
}

ast::Expression *CodeGen::AggPartitionIteratorNext(ast::Expression *iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggPartIterNext, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::AggPartitionIteratorGetHash(ast::Expression *iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggPartIterGetHash, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt64));
  return call;
}

ast::Expression *CodeGen::AggPartitionIteratorGetRow(ast::Expression *iter,
                                                     ast::Identifier agg_payload_type) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggPartIterGetRow, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt8)->PointerTo());
  return PtrCast(agg_payload_type, call);
}

ast::Expression *CodeGen::AggPartitionIteratorGetRowEntry(ast::Expression *iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggPartIterGetRowEntry, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::HashTableEntry)->PointerTo());
  return call;
}

ast::Expression *CodeGen::AggHashTableIteratorInit(ast::Expression *iter, ast::Expression *agg_ht) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggHashTableIterInit, {iter, agg_ht});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::AggHashTableIteratorHasNext(ast::Expression *iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggHashTableIterHasNext, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return call;
}

ast::Expression *CodeGen::AggHashTableIteratorNext(ast::Expression *iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggHashTableIterNext, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::AggHashTableIteratorGetRow(ast::Expression *iter,
                                                     ast::Identifier agg_payload_type) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggHashTableIterGetRow, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt8)->PointerTo());
  return PtrCast(agg_payload_type, call);
}

ast::Expression *CodeGen::AggHashTableIteratorClose(ast::Expression *iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggHashTableIterClose, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Aggregators
// ---------------------------------------------------------

ast::Expression *CodeGen::AggregatorInit(ast::Expression *agg) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggInit, {agg});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::AggregatorAdvance(ast::Expression *agg, ast::Expression *val) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggAdvance, {agg, val});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::AggregatorMerge(ast::Expression *agg1, ast::Expression *agg2) {
  ast::Expression *call = CallBuiltin(ast::Builtin::AggMerge, {agg1, agg2});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::AggregatorResult(ast::Expression *agg) {
  return CallBuiltin(ast::Builtin::AggResult, {agg});
}

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

ast::Expression *CodeGen::SorterInit(ast::Expression *sorter, ast::Expression *mem_pool,
                                     ast::Identifier cmp_func_name,
                                     ast::Identifier sort_row_type_name) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::SorterInit,
                  {sorter, mem_pool, MakeExpr(cmp_func_name), SizeOf(sort_row_type_name)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::SorterInsert(ast::Expression *sorter,
                                       ast::Identifier sort_row_type_name) {
  // @sorterInsert(sorter)
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterInsert, {sorter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt8)->PointerTo());
  // @ptrCast(sort_row_type, @sorterInsert())
  return PtrCast(sort_row_type_name, call);
}

ast::Expression *CodeGen::SorterInsertTopK(ast::Expression *sorter,
                                           ast::Identifier sort_row_type_name, uint64_t top_k) {
  // @sorterInsertTopK(sorter)
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterInsertTopK, {sorter, Const64(top_k)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt8)->PointerTo());
  // @ptrCast(sort_row_type, @sorterInsertTopK())
  return PtrCast(sort_row_type_name, call);
}

ast::Expression *CodeGen::SorterInsertTopKFinish(ast::Expression *sorter, uint64_t top_k) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::SorterInsertTopKFinish, {sorter, Const64(top_k)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::SorterSort(ast::Expression *sorter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterSort, {sorter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::SortParallel(ast::Expression *sorter, ast::Expression *tls,
                                       ast::Expression *offset) {
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterSortParallel, {sorter, tls, offset});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::SortTopKParallel(ast::Expression *sorter, ast::Expression *tls,
                                           ast::Expression *offset, std::size_t top_k) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::SorterSortTopKParallel, {sorter, tls, offset, Const64(top_k)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::SorterFree(ast::Expression *sorter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterFree, {sorter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::SorterIterInit(ast::Expression *iter, ast::Expression *sorter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterIterInit, {iter, sorter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::SorterIterHasNext(ast::Expression *iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterIterHasNext, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return call;
}

ast::Expression *CodeGen::SorterIterNext(ast::Expression *iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterIterNext, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::SorterIterSkipRows(ast::Expression *iter, uint32_t n) {
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterIterSkipRows, {iter, Const64(n)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::SorterIterGetRow(ast::Expression *iter, ast::Identifier row_type_name) {
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterIterGetRow, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt8)->PointerTo());
  return PtrCast(row_type_name, call);
}

ast::Expression *CodeGen::SorterIterClose(ast::Expression *iter) {
  ast::Expression *call = CallBuiltin(ast::Builtin::SorterIterClose, {iter});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// SQL functions
// ---------------------------------------------------------

ast::Expression *CodeGen::Like(ast::Expression *str, ast::Expression *pattern) {
  ast::Expression *call = CallBuiltin(ast::Builtin::Like, {str, pattern});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return call;
}

ast::Expression *CodeGen::NotLike(ast::Expression *str, ast::Expression *pattern) {
  return UnaryOp(parsing::Token::Type::BANG, Like(str, pattern));
}

// ---------------------------------------------------------
// CSV
// ---------------------------------------------------------

ast::Expression *CodeGen::CSVReaderInit(ast::Expression *reader, std::string_view file_name) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::CSVReaderInit, {reader, ConstString(file_name)});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return call;
}

ast::Expression *CodeGen::CSVReaderAdvance(ast::Expression *reader) {
  ast::Expression *call = CallBuiltin(ast::Builtin::CSVReaderAdvance, {reader});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return call;
}

ast::Expression *CodeGen::CSVReaderGetField(ast::Expression *reader, uint32_t field_index,
                                            ast::Expression *result) {
  ast::Expression *call =
      CallBuiltin(ast::Builtin::CSVReaderGetField, {reader, Const32(field_index), result});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

ast::Expression *CodeGen::CSVReaderGetRecordNumber(ast::Expression *reader) {
  ast::Expression *call = CallBuiltin(ast::Builtin::CSVReaderGetRecordNumber, {reader});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::UInt32));
  return call;
}

ast::Expression *CodeGen::CSVReaderClose(ast::Expression *reader) {
  ast::Expression *call = CallBuiltin(ast::Builtin::CSVReaderClose, {reader});
  call->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Extras
// ---------------------------------------------------------

ast::Identifier CodeGen::MakeFreshIdentifier(std::string_view str) {
  return Context()->GetIdentifier(scope_->GetFreshName(str));
}

ast::Identifier CodeGen::MakeIdentifier(std::string_view str) const {
  return Context()->GetIdentifier(str);
}

ast::Expression *CodeGen::MakeExpr(ast::Identifier ident) const {
  return NodeFactory()->NewIdentifierExpression(position_, ident);
}

ast::Statement *CodeGen::MakeStatement(ast::VariableDeclaration *var) const {
  return NodeFactory()->NewDeclStatement(var);
}

ast::Statement *CodeGen::MakeStatement(ast::Expression *expr) const {
  return NodeFactory()->NewExpressionStatement(expr);
}

ast::BlockStatement *CodeGen::MakeEmptyBlock() const {
  return NodeFactory()->NewBlockStatement(position_, position_, {{}, Context()->GetRegion()});
}

util::RegionVector<ast::FieldDeclaration *> CodeGen::MakeEmptyFieldList() const {
  return util::RegionVector<ast::FieldDeclaration *>(Context()->GetRegion());
}

util::RegionVector<ast::FieldDeclaration *> CodeGen::MakeFieldList(
    std::initializer_list<ast::FieldDeclaration *> fields) const {
  return util::RegionVector<ast::FieldDeclaration *>(fields, Context()->GetRegion());
}

ast::FieldDeclaration *CodeGen::MakeField(ast::Identifier name, ast::Expression *type) const {
  return NodeFactory()->NewFieldDeclaration(position_, name, type);
}

ast::FieldDeclaration *CodeGen::MakeField(std::string_view name, ast::Expression *type) {
  return MakeField(MakeFreshIdentifier(name), type);
}

ast::AstNodeFactory *CodeGen::NodeFactory() const { return Context()->GetNodeFactory(); }

void CodeGen::EnterScope() {
  if (num_cached_scopes_ == 0) {
    scope_ = new LexicalScope(scope_);
  } else {
    auto scope = scope_cache_[--num_cached_scopes_].release();
    scope->Init(scope_);
    scope_ = scope;
  }
}

void CodeGen::ExitScope() {
  LexicalScope *scope = scope_;
  scope_ = scope->Previous();

  if (num_cached_scopes_ < kDefaultScopeCacheSize) {
    scope_cache_[num_cached_scopes_++].reset(scope);
  } else {
    delete scope;
  }
}

ast::Expression *CodeGen::Deref(ast::Expression *ptr) const {
  ast::Expression *result = UnaryOp(parsing::Token::Type::STAR, ptr);
  result->SetType(ptr->GetType()->GetPointeeType());
  return result;
}

ast::Expression *CodeGen::MakeExpr(ast::Identifier name, ast::Type *type) {
  ast::Expression *result = MakeExpr(name);
  result->SetType(type);
  return result;
}

namespace {

class TypeReprBuilder : public ast::TypeVisitor<TypeReprBuilder, ast::Expression *> {
 public:
  explicit TypeReprBuilder(SourcePosition pos, ast::Context *context) : context_(context) {}

  ast::Expression *GetRepresentation(const ast::Type *type) { return Visit(type); }

#define DECLARE_VISIT_TYPE(Type) ast::Expression *Visit##Type(const ast::Type *type);
  TYPE_LIST(DECLARE_VISIT_TYPE)
#undef DECLARE_VISIT_TYPE

 private:
  ast::AstNodeFactory *GetFactory() { return context_->GetNodeFactory(); }

 private:
  SourcePosition pos_;
  ast::Context *context_;
};

ast::Expression *TypeReprBuilder::VisitPointerType(const ast::PointerType *type) {
  return GetFactory()->NewPointerType(pos_, GetRepresentation(type->GetBase()));
}

ast::Expression *TypeReprBuilder::VisitBuiltinType(const ast::BuiltinType *type) {
  return GetFactory()->NewIdentifierExpression(pos_, context_->GetIdentifier(type->GetTplName()));
}

ast::Expression *TypeReprBuilder::VisitArrayType(const ast::ArrayType *type) {
  ast::Expression *length = nullptr, *element_type = GetRepresentation(type->GetElementType());
  if (type->HasKnownLength()) {
    length = GetFactory()->NewIntLiteral(pos_, type->GetLength());
  }
  return GetFactory()->NewArrayType(pos_, length, element_type);
}

ast::Expression *TypeReprBuilder::VisitMapType(const ast::MapType *type) {
  ast::Expression *key_type = GetRepresentation(type->GetKeyType());
  ast::Expression *val_type = GetRepresentation(type->GetValueType());
  return GetFactory()->NewMapType(pos_, key_type, val_type);
}
ast::Expression *TypeReprBuilder::VisitStructType(const ast::StructType *type) {
  util::RegionVector<ast::FieldDeclaration *> fields(context_->GetRegion());
  fields.reserve(type->GetFields().size());
  for (const auto &field : type->GetFields()) {
    fields.emplace_back(
        GetFactory()->NewFieldDeclaration(pos_, field.name, GetRepresentation(field.type)));
  }
  return GetFactory()->NewStructType(pos_, std::move(fields));
}

ast::Expression *TypeReprBuilder::VisitStringType(const ast::StringType *type) {
  // TODO(pmenon): Fill me in.
  return TypeVisitor::VisitStringType(type);
}

ast::Expression *TypeReprBuilder::VisitFunctionType(const ast::FunctionType *type) {
  // The parameters.
  util::RegionVector<ast::FieldDeclaration *> params(context_->GetRegion());
  params.reserve(type->GetNumParams());
  for (const auto &field : type->GetParams()) {
    params.emplace_back(
        GetFactory()->NewFieldDeclaration(pos_, field.name, GetRepresentation(field.type)));
  }
  // The return type.
  ast::Expression *ret_type = GetRepresentation(type->GetReturnType());
  // Done.
  return GetFactory()->NewFunctionType(pos_, std::move(params), ret_type);
}

}  // namespace

ast::VariableDeclaration *CodeGen::DeclareVar(ast::Identifier name, ast::Type *type) {
  TypeReprBuilder builder(position_, Context());
  ast::Expression *type_repr = builder.GetRepresentation(type);
  type_repr->SetType(type);
  return NodeFactory()->NewVariableDeclaration(position_, name, type_repr, nullptr);
}

ast::Expression *CodeGen::BinaryMathOp(parsing::Token::Type op, ast::Expression *lhs,
                                       ast::Expression *rhs) {
  TPL_ASSERT(lhs->GetType() != nullptr && lhs->GetType() == rhs->GetType(),
             "Missing or mismatched types in binary math operation.");
  TPL_ASSERT(lhs->GetType()->IsArithmetic(), "Binary operation on non-arithmetic inputs!");
  TPL_ASSERT(op != parsing::Token::Type::PERCENT || lhs->GetType()->IsFloatType(),
             "Floating point operation not allowed.");
  ast::Expression *result = BinaryOp(op, lhs, rhs);
  result->SetType(lhs->GetType());
  return result;
}

ast::Expression *CodeGen::ComparisonOp(parsing::Token::Type op, ast::Expression *lhs,
                                       ast::Expression *rhs) {
  ast::Expression *result = Compare(op, lhs, rhs);
  result->SetType(ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool));
  return result;
}

ast::Expression *CodeGen::Add(ast::Expression *lhs, ast::Expression *rhs) {
  return BinaryMathOp(parsing::Token::Type::PLUS, lhs, rhs);
}

ast::Expression *CodeGen::Sub(ast::Expression *lhs, ast::Expression *rhs) {
  return BinaryMathOp(parsing::Token::Type::MINUS, lhs, rhs);
}

ast::Expression *CodeGen::Mul(ast::Expression *lhs, ast::Expression *rhs) {
  return BinaryMathOp(parsing::Token::Type::STAR, lhs, rhs);
}

ast::Expression *CodeGen::Div(ast::Expression *lhs, ast::Expression *rhs) {
  return BinaryMathOp(parsing::Token::Type::SLASH, lhs, rhs);
}

ast::Expression *CodeGen::Mod(ast::Expression *lhs, ast::Expression *rhs) {
  return BinaryMathOp(parsing::Token::Type::PERCENT, lhs, rhs);
}

ast::Expression *CodeGen::CompareEq(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::EQUAL_EQUAL, lhs, rhs);
}

ast::Expression *CodeGen::CompareNe(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::BANG_EQUAL, lhs, rhs);
}

ast::Expression *CodeGen::CompareLt(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::LESS, lhs, rhs);
}

ast::Expression *CodeGen::CompareLe(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::LESS_EQUAL, lhs, rhs);
}

ast::Expression *CodeGen::CompareGt(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::GREATER, lhs, rhs);
}

ast::Expression *CodeGen::CompareGe(ast::Expression *lhs, ast::Expression *rhs) {
  return ComparisonOp(parsing::Token::Type::GREATER_EQUAL, lhs, rhs);
}

}  // namespace tpl::sql::codegen
