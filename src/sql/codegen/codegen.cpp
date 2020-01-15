#include "sql/codegen/codegen.h"

#include "ast/ast_node_factory.h"
#include "ast/builtins.h"
#include "ast/context.h"
#include "ast/type.h"
#include "sql/codegen/code_container.h"

namespace tpl::sql::codegen {

//===----------------------------------------------------------------------===//
//
// Scopes
//
//===----------------------------------------------------------------------===//

// TODO(pmenon): Fix me!
llvm::StringRef CodeGen::Scope::GetFreshName(const std::string &name) {
  // Attempt insert.
  auto insert_result = names_.insert(std::make_pair(name, 1));
  if (insert_result.second) {
    return insert_result.first->getKey();
  }
  // Duplicate found.
  auto result = name + std::to_string(insert_result.first->getValue()++);
  return result;
}

//===----------------------------------------------------------------------===//
//
// Code Generator
//
//===----------------------------------------------------------------------===//

CodeGen::CodeGen(ast::Context *context)
    : context_(context),
      position_{0, 0},
      curr_function_(nullptr),
      id_counter_(0),
      num_cached_scopes_(0),
      scope_(nullptr) {
  for (auto &scope : scope_cache_) {
    scope = std::make_unique<Scope>(nullptr);
  }
  num_cached_scopes_ = kDefaultScopeCacheSize;
  EnterScope();
}

CodeGen::~CodeGen() { ExitScope(); }

ast::Expr *CodeGen::ConstBool(bool val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewBoolLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return expr;
}

ast::Expr *CodeGen::Const8(int8_t val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int8));
  return expr;
}

ast::Expr *CodeGen::Const16(int16_t val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int16));
  return expr;
}

ast::Expr *CodeGen::Const32(int32_t val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int32));
  return expr;
}

ast::Expr *CodeGen::Const64(int64_t val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int64));
  return expr;
}

ast::Expr *CodeGen::ConstDouble(double val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewFloatLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Float32));
  return expr;
}

ast::Expr *CodeGen::ConstString(std::string_view str) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewStringLiteral(position_, MakeIdentifier(str));
  expr->SetType(ast::StringType::Get(context_));
  return expr;
}

ast::VariableDecl *CodeGen::DeclareVar(ast::Identifier name, ast::Expr *type_repr,
                                       ast::Expr *init) {
  // Create a unique name for the variable
  ast::IdentifierExpr *var_name = MakeExpr(name);
  // Build and append the declaration
  return context_->GetNodeFactory()->NewVariableDecl(position_, var_name->Name(), type_repr, init);
}

ast::VariableDecl *CodeGen::DeclareVarNoInit(ast::Identifier name, ast::Expr *type_repr) {
  return DeclareVar(name, type_repr, nullptr);
}

ast::VariableDecl *CodeGen::DeclareVarNoInit(ast::Identifier name, ast::BuiltinType::Kind kind) {
  return DeclareVarNoInit(name, BuiltinType(kind));
}

ast::VariableDecl *CodeGen::DeclareVarWithInit(ast::Identifier name, ast::Expr *init) {
  return DeclareVar(name, nullptr, init);
}

ast::StructDecl *CodeGen::DeclareStruct(ast::Identifier name,
                                        util::RegionVector<ast::FieldDecl *> &&fields) const {
  // The type representation
  ast::StructTypeRepr *type =
      context_->GetNodeFactory()->NewStructType(position_, std::move(fields));
  // The declaration
  ast::StructDecl *decl = context_->GetNodeFactory()->NewStructDecl(position_, name, type);
  // Done
  return decl;
}

ast::Stmt *CodeGen::Assign(ast::Expr *dest, ast::Expr *value) {
  // TODO(pmenon): Check types?
  // Set the type of the destination
  dest->SetType(value->GetType());
  // Done.
  return context_->GetNodeFactory()->NewAssignmentStmt(position_, dest, value);
}

ast::Expr *CodeGen::BuiltinType(ast::BuiltinType::Kind builtin_kind) const {
  // Lookup the builtin type. We'll use it to construct an identifier.
  ast::BuiltinType *type = ast::BuiltinType::Get(context_, builtin_kind);
  // Build an identifier expression using the builtin's name
  ast::Expr *expr = MakeExpr(context_->GetIdentifier(type->GetTplName()));
  // Set the type to avoid double-checking the type
  expr->SetType(type);
  // Done
  return expr;
}

ast::Expr *CodeGen::Int8Type() const { return BuiltinType(ast::BuiltinType::Int8); }

ast::Expr *CodeGen::Int16Type() const { return BuiltinType(ast::BuiltinType::Int16); }

ast::Expr *CodeGen::Int32Type() const { return BuiltinType(ast::BuiltinType::Int32); }

ast::Expr *CodeGen::Int64Type() const { return BuiltinType(ast::BuiltinType::Int64); }

ast::Expr *CodeGen::Float32Type() const { return BuiltinType(ast::BuiltinType::Float32); }

ast::Expr *CodeGen::Float64Type() const { return BuiltinType(ast::BuiltinType::Float64); }

ast::Expr *CodeGen::PointerType(ast::Expr *base_type_repr) const {
  // Create the type representation
  auto *type_repr = context_->GetNodeFactory()->NewPointerType(position_, base_type_repr);
  // Set the actual TPL type
  if (base_type_repr->GetType() != nullptr) {
    type_repr->SetType(ast::PointerType::Get(base_type_repr->GetType()));
  }
  // Done
  return type_repr;
}

ast::Expr *CodeGen::PointerType(ast::Identifier type_name) const {
  return PointerType(MakeExpr(type_name));
}

ast::Expr *CodeGen::PointerType(ast::BuiltinType::Kind builtin) const {
  return PointerType(BuiltinType(builtin));
}

ast::Expr *CodeGen::TplType(sql::TypeId type) {
  switch (type) {
    case sql::TypeId::Boolean:
      return BuiltinType(ast::BuiltinType::Kind::Boolean);
    case sql::TypeId::TinyInt:
    case sql::TypeId::SmallInt:
    case sql::TypeId::Integer:
    case sql::TypeId::BigInt:
      return BuiltinType(ast::BuiltinType::Kind::Integer);
    case sql::TypeId::Date:
      return BuiltinType(ast::BuiltinType::Kind::Date);
    case sql::TypeId::Double:
    case sql::TypeId::Float:
      return BuiltinType(ast::BuiltinType::Kind::Real);
    case sql::TypeId::Varchar:
      return BuiltinType(ast::BuiltinType::Kind::StringVal);
    default:
      UNREACHABLE("Cannot codegen unsupported type.");
  }
}

ast::Expr *CodeGen::Nil() const {
  ast::Expr *expr = context_->GetNodeFactory()->NewNilLiteral(position_);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return expr;
}

ast::Expr *CodeGen::AddressOf(ast::Expr *obj) const {
  return UnaryOp(parsing::Token::Type::AMPERSAND, obj);
}

ast::Expr *CodeGen::SizeOf(ast::Identifier type_name) const {
  return CallBuiltin(ast::Builtin::SizeOf, {MakeExpr(type_name)});
}

ast::Expr *CodeGen::OffsetOf(ast::Identifier obj, ast::Identifier member) const {
  return CallBuiltin(ast::Builtin::OffsetOf, {MakeExpr(obj), MakeExpr(member)});
}

ast::Expr *CodeGen::BuildCall(ast::Identifier func_name,
                              std::initializer_list<ast::Expr *> args) const {
  // Create the call argument list
  util::RegionVector<ast::Expr *> call_args(args, context_->GetRegion());
  // Invoke the function
  return context_->GetNodeFactory()->NewCallExpr(MakeExpr(func_name), std::move(call_args));
}

ast::Expr *CodeGen::BinaryOp(parsing::Token::Type op, ast::Expr *left, ast::Expr *right) const {
  TPL_ASSERT(parsing::Token::IsBinaryOp(op), "Provided operation isn't binary");
  return context_->GetNodeFactory()->NewBinaryOpExpr(position_, op, left, right);
}

ast::Expr *CodeGen::Compare(parsing::Token::Type op, ast::Expr *left, ast::Expr *right) const {
  return context_->GetNodeFactory()->NewComparisonOpExpr(position_, op, left, right);
}

ast::Expr *CodeGen::UnaryOp(parsing::Token::Type op, ast::Expr *input) const {
  return context_->GetNodeFactory()->NewUnaryOpExpr(position_, op, input);
}

ast::Expr *CodeGen::AccessStructMember(ast::Expr *object, ast::Identifier member) {
  return context_->GetNodeFactory()->NewMemberExpr(position_, object, MakeExpr(member));
}

ast::Stmt *CodeGen::Return() {
  return Return(nullptr);
}

ast::Stmt *CodeGen::Return(ast::Expr *ret) {
  ast::Stmt *stmt = context_->GetNodeFactory()->NewReturnStmt(position_, ret);
  NewLine();
  return stmt;
}

ast::Expr *CodeGen::Call(ast::Identifier func_name, std::initializer_list<ast::Expr *> args) const {
  return BuildCall(func_name, args);
}

ast::Expr *CodeGen::CallBuiltin(ast::Builtin builtin,
                                std::initializer_list<ast::Expr *> args) const {
  util::RegionVector<ast::Expr *> call_args(args, context_->GetRegion());
  ast::Expr *func = MakeExpr(context_->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
  ast::Expr *call = context_->GetNodeFactory()->NewBuiltinCallExpr(func, std::move(call_args));
  return call;
}

ast::Expr *CodeGen::BoolToSql(bool b) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::BoolToSql, {ConstBool(b)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Boolean));
  return call;
}

ast::Expr *CodeGen::IntToSql(int64_t num) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::IntToSql, {Const64(num)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Integer));
  return call;
}

ast::Expr *CodeGen::FloatToSql(int64_t num) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::FloatToSql, {ConstDouble(num)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Real));
  return call;
}

ast::Expr *CodeGen::DateToSql(Date date) const {
  uint32_t year, month, day;
  date.ExtractComponents(&year, &month, &day);
  return DateToSql(year, month, day);
}

ast::Expr *CodeGen::DateToSql(uint32_t year, uint32_t month, uint32_t day) const {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::DateToSql, {Const32(year), Const32(month), Const32(day)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Date));
  return call;
}

ast::Expr *CodeGen::StringToSql(std::string_view str) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::StringToSql, {ConstString(str)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::StringVal));
  return call;
}

ast::Expr *CodeGen::TableIterInit(ast::Expr *table_iter, std::string_view table_name) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::TableIterInit, {table_iter, ConstString(table_name)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::TableIterAdvance(ast::Expr *table_iter) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::TableIterAdvance, {table_iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::TableIterGetVPI(ast::Expr *table_iter) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::TableIterGetVPI, {table_iter});
  call->SetType(
      ast::BuiltinType::Get(context_, ast::BuiltinType::VectorProjectionIterator)->PointerTo());
  return call;
}

ast::Expr *CodeGen::TableIterClose(ast::Expr *table_iter) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::TableIterClose, {table_iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::IterateTableParallel(std::string_view table_name, ast::Expr *query_state,
                                         ast::Expr *tls, ast::Identifier worker_name) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::TableIterParallel,
                                {ConstString(table_name), query_state, tls, MakeExpr(worker_name)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::ExecCtxGetMemoryPool(ast::Expr *exec_ctx) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextGetMemoryPool, {exec_ctx});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::MemoryPool)->PointerTo());
  return call;
}

ast::Expr *CodeGen::ExecCtxGetTLS(ast::Expr *exec_ctx) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextGetTLS, {exec_ctx});
  call->SetType(
      ast::BuiltinType::Get(context_, ast::BuiltinType::ThreadStateContainer)->PointerTo());
  return call;
}

ast::Expr *CodeGen::TLSReset(ast::Expr *tls, ast::Identifier tls_state_name,
                             ast::Identifier init_fn, ast::Identifier tear_down_fn,
                             ast::Expr *context) const {
  ast::Expr *call = CallBuiltin(
      ast::Builtin::ThreadStateContainerReset,
      {tls, SizeOf(tls_state_name), MakeExpr(init_fn), MakeExpr(tear_down_fn), context});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

ast::Expr *CodeGen::SorterInit(ast::Expr *sorter, ast::Expr *mem_pool,
                               ast::Identifier cmp_func_name,
                               ast::Identifier sort_row_type_name) const {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::SorterInit,
                  {sorter, mem_pool, MakeExpr(cmp_func_name), SizeOf(sort_row_type_name)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterSort(ast::Expr *sorter) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterSort, {sorter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SortParallel(ast::Expr *sorter, ast::Expr *tls, ast::Expr *offset) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterSortParallel, {sorter, tls, offset});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SortTopKParallel(ast::Expr *sorter, ast::Expr *tls, ast::Expr *offset,
                                     std::size_t top_k) const {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::SorterSortParallel, {sorter, tls, offset, Const64(top_k)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterFree(ast::Expr *sorter) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterFree, {sorter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterIterInit(ast::Expr *iter, ast::Expr *sorter) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterInit, {iter, sorter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterIterHasNext(ast::Expr *iter) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterHasNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::SorterIterNext(ast::Expr *iter) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterIterGetRow(ast::Expr *iter) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterGetRow, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  return call;
}

ast::Expr *CodeGen::SorterIterClose(ast::Expr *iter) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterClose, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// SQL functions
// ---------------------------------------------------------

ast::Expr *CodeGen::Like(ast::Expr *str, ast::Expr *pattern) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::Like, {str, pattern});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::NotLike(ast::Expr *str, ast::Expr *pattern) const {
  return UnaryOp(parsing::Token::Type::BANG, Like(str, pattern));
}

ast::Identifier CodeGen::MakeFreshIdentifier(const std::string &str) {
  return context_->GetIdentifier(scope_->GetFreshName(str));
}

ast::Identifier CodeGen::MakeIdentifier(std::string_view str) const {
  return context_->GetIdentifier({str.data(), str.length()});
}

ast::IdentifierExpr *CodeGen::MakeExpr(ast::Identifier ident) const {
  return context_->GetNodeFactory()->NewIdentifierExpr(position_, ident);
}

ast::BlockStmt *CodeGen::MakeEmptyBlock() const {
  return context_->GetNodeFactory()->NewBlockStmt(position_, position_,
                                                  {{}, context_->GetRegion()});
}

util::RegionVector<ast::FieldDecl *> CodeGen::MakeEmptyFieldList() const {
  return util::RegionVector<ast::FieldDecl *>(context_->GetRegion());
}

util::RegionVector<ast::FieldDecl *> CodeGen::MakeFieldList(
    std::initializer_list<ast::FieldDecl *> fields) const {
  return util::RegionVector<ast::FieldDecl *>(fields, context_->GetRegion());
}

ast::FieldDecl *CodeGen::MakeField(ast::Identifier name, ast::Expr *type) const {
  return context_->GetNodeFactory()->NewFieldDecl(position_, name, type);
}

ast::AstNodeFactory *CodeGen::GetFactory() { return context_->GetNodeFactory(); }

void CodeGen::EnterScope() {
  if (num_cached_scopes_ == 0) {
    scope_ = new Scope(scope_);
  } else {
    auto scope = scope_cache_[--num_cached_scopes_].release();
    scope->Init(scope_);
    scope_ = scope;
  }
}

void CodeGen::ExitScope() {
  Scope *scope = scope_;
  scope_ = scope->Previous();

  if (num_cached_scopes_ < kDefaultScopeCacheSize) {
    scope_cache_[num_cached_scopes_++].reset(scope);
  } else {
    delete scope;
  }
}

}  // namespace tpl::sql::codegen
