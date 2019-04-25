//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// code_context.cpp
//
// Identification: src/codegen/code_context.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "compiler/code_context.h"
#include <ast/type.h>

#include "common/exception.h"

#include "sema/error_reporter.h"

namespace tpl {
namespace compiler {

/// Atomic plan ID counter
static std::atomic<uint64_t> kIdCounter{0};

////////////////////////////////////////////////////////////////////////////////
///
/// Code Context
///
////////////////////////////////////////////////////////////////////////////////

/// Constructor
CodeContext::CodeContext(sema::ErrorReporter &errorReporter)
    : id_(kIdCounter++),
      func_(nullptr),
      udf_func_ptr_(nullptr),
      region_(),
      ast_context_(region_, &errorReporter),
      is_verified_(false) {
  // Setup the common types we need once
  SourcePosition dummy;
  bool_type_ = ast_context_.node_factory()->NewIdentifierExpr(
      dummy, ast::Identifier(
                 ast::BuiltinType::Get(&ast_context_, ast::BuiltinType::Bool)
                     ->tpl_name()));
  int8_type_ = ast_context_.node_factory()->NewIdentifierExpr(
      dummy, ast::Identifier(
                 ast::BuiltinType::Get(&ast_context_, ast::BuiltinType::Int8)
                     ->tpl_name()));
  int16_type_ = ast_context_.node_factory()->NewIdentifierExpr(
      dummy, ast::Identifier(
                 ast::BuiltinType::Get(&ast_context_, ast::BuiltinType::Int16)
                     ->tpl_name()));
  int32_type_ = ast_context_.node_factory()->NewIdentifierExpr(
      dummy, ast::Identifier(
                 ast::BuiltinType::Get(&ast_context_, ast::BuiltinType::Int32)
                     ->tpl_name()));
  int64_type_ = ast_context_.node_factory()->NewIdentifierExpr(
      dummy, ast::Identifier(
                 ast::BuiltinType::Get(&ast_context_, ast::BuiltinType::Int64)
                     ->tpl_name()));
  double_type_ = ast_context_.node_factory()->NewIdentifierExpr(
      dummy, ast::Identifier(
                 ast::BuiltinType::Get(&ast_context_, ast::BuiltinType::Float64)
                     ->tpl_name()));
  float_type_ = ast_context_.node_factory()->NewIdentifierExpr(
      dummy, ast::Identifier(
                 ast::BuiltinType::Get(&ast_context_, ast::BuiltinType::Float32)
                     ->tpl_name()));
  /*void_type_ = ast_context_.node_factory()->NewIdentifierExpr(
      dummy, ast::Identifier(
          ast::BuiltinType::Get(&ast_context_, ast::Type::)
              ->tpl_name()));
  void_ptr_type_ = llvm::Type::getInt8PtrTy(*context_);
  char_ptr_type_ = llvm::Type::getInt8PtrTy(*context_);*/
}

/// Destructor
CodeContext::~CodeContext() {
  // We need this empty constructor because we declared a std::unique_ptr<>
  // on llvm::ExecutionEngine and llvm::LLVMContext that are forward-declared
  // in the header file. To make this compile, this destructor needs to exist.
}

Type *CodeContext::TypeFromTypeId(sql::TypeId typeId) {
  // TODO (tanujnay112) find a better way to do this
  switch (typeId) {
    case sql::TypeId::Boolean:
      return bool_type_;
    case sql::TypeId::SmallInt:
      return int8_type_;
    case sql::TypeId::Integer:
      return int32_type_;
    case sql::TypeId::BigInt:
      return int64_type_;
    case sql::TypeId::Decimal:
      return float_type_;
    default:
      TPL_ASSERT(false, "Type unsupported so far");
  }
  return nullptr;
}

void CodeContext::RegisterFunction(Function *func) {
  functions_.emplace_back(func);
}

void CodeContext::RegisterBuiltin(Function *func_decl,
                                  CodeContext::FuncPtr func_impl) {
  const auto name = func_decl->GetName();

  // Register the builtin function with type and implementation
  builtins_[name] = std::make_pair(func_decl, func_impl);
}

/// Verify all the functions that were created in this context
void CodeContext::Verify() {
  // Verify the module is okay

  // All is well
  is_verified_ = true;
}

/// Optimize all the functions that were created in this context
void CodeContext::Optimize() {
  // make sure the code is verified
  if (!is_verified_) Verify();
}

/// JIT compile all the functions that were created in this context
void CodeContext::Compile() {
  // make sure the code is verified
  if (!is_verified_) Verify();

  // go through vec and compile

  for (auto fn : functions_) {
    fn->Compile();
  }
}

void CodeContext::DumpContents() const {}

}  // namespace compiler
}  // namespace tpl