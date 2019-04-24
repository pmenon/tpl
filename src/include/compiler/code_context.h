//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// code_context.h
//
// Identification: src/include/codegen/code_context.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <ast/context.h>
#include <string>
#include <unordered_map>

#include "common/macros.h"
#include "compiler/code_builder.h"

namespace tpl {
namespace compiler {

class FunctionBuilder;

namespace interpreter {
class BytecodeBuilder;
}  // namespace interpreter

//===----------------------------------------------------------------------===//
// The context where all generated LLVM query code resides. We create a context
// instance for every query we see.  We keep instances of these around in the
// off-chance that we see a query that requires a function we've previously
// JITed. In reality, this is a thin wrapper around an LLVM Module.
//===----------------------------------------------------------------------===//
class CodeContext {
  friend class CodeGen;
  friend class FunctionBuilder;
  friend class interpreter::BytecodeBuilder;

 public:
  using FuncPtr = void *;

  CodeContext(sema::ErrorReporter &errorReporter);
  ~CodeContext();

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(CodeContext);

  /// Register a function that will be defined in this context
  void RegisterFunction(Function *func);

  /// Register a function that is defined externally
  void RegisterExternalFunction(Function *func_decl, FuncPtr func_impl);

  /// Register a built-in C/C++ function
  void RegisterBuiltin(Function *func_decl, FuncPtr func_impl);

  /// Return the LLVM function for UDF that has been registered in this context
  Function *GetUDF() const { return udf_func_ptr_; }

  /// Sets UDF function ptr
  void SetUDF(Function *func_ptr) { udf_func_ptr_ = func_ptr; }

  /// Verify all the code contained in this context
  void Verify();

  /// Optimize all the code contained in this context
  void Optimize();

  /// Compile all the code contained in this context
  void Compile();

  /// Dump the contents of all the code in this context
  /// Attention: this function may change the IR!
  void DumpContents() const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// Get the globally unique identifier for this code
  uint64_t GetID() const { return id_; }

 private:
  // Set the current function we're building
  void SetCurrentFunction(Function *func) { func_ = func; }

  // Get the current function we're building
  Function *GetCurrentFunction() const { return func_; }

 private:
  // The ID/version of code
  uint64_t id_;

  // The function that is currently being generated
  Function *func_;

  // The llvm::Function ptr of the outermost function built
  Function *udf_func_ptr_;

  util::Region *region_;
  ast::Context ast_context_;

  // Handy types we reuse often enough to cache here
  Type *bool_type_;
  Type *int8_type_;
  Type *int16_type_;
  Type *int32_type_;
  Type *int64_type_;
  Type *float_type_;
  Type *double_type_;
  Type *void_type_;
  Type *void_ptr_type_;

  // All C/C++ builtin functions and their implementations
  std::unordered_map<std::string, std::pair<Function *, FuncPtr>> builtins_;

  // The functions needed in this module, and their implementations. If the
  // function has not been compiled yet, the function pointer will be NULL. The
  // function pointers are populated in Compile()
  std::vector<Function *> functions_;

  // Shows if the Verify() has been run
  bool is_verified_;
};

}  // namespace compiler
}  // namespace tpl
