#pragma once

#include "llvm/ADT/SmallVector.h"

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/code_container.h"
#include "sql/codegen/query_state.h"

namespace tpl::sql::codegen {

/**
 * A structure capturing all top-level declarations needed by a query during code generation.
 */
class TopLevelDeclarations {
 public:
  /**
   * Register the given struct as a top-level structure.
   * @param struct_decl The declaration.
   */
  void RegisterStruct(ast::StructDecl *struct_decl) { structs_.push_back(struct_decl); }

  /**
   * Register the given function as a top-level function.
   * @param function_decl The function.
   */
  void RegisterFunction(ast::FunctionDecl *function_decl) { functions_.push_back(function_decl); }

  /**
   * Make all top-level declarations available in the provided code container.
   * @param container The container to add our declarations into.
   */
  void DeclareInContainer(CodeContainer *container) {
    for (auto *decl : structs_) {
      container->RegisterStruct(decl);
    }
    for (auto *decl : functions_) {
      container->RegisterFunction(decl);
    }
  }

 private:
  // The list of structures.
  llvm::SmallVector<ast::StructDecl *, 8> structs_;
  // The list of functions.
  llvm::SmallVector<ast::FunctionDecl *, 32> functions_;
};

}  // namespace tpl::sql::codegen
