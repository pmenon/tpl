#pragma once

#include <memory>

#include "common/common.h"
#include "common/macros.h"
#include "sql/codegen/ast_fwd.h"
#include "util/region_containers.h"

namespace tpl::vm {
class Module;
}  // namespace tpl::vm

namespace tpl::sql::codegen {

/**
 * A container for code in a single TPL file.
 */
class CodeContainer {
  friend class CodeGen;

 public:
  /**
   * Create a new TPL code container.
   * @param ctx The AST context to use.
   */
  explicit CodeContainer(ast::Context *ctx);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(CodeContainer);

  /**
   * Destructor.
   */
  ~CodeContainer();

  /**
   * Register the given struct in this container.
   * @param decl The struct declaration.
   */
  void RegisterStruct(ast::StructDecl *decl);

  /**
   * Register the given function in this container;
   * @param decl The function declaration.
   */
  void RegisterFunction(ast::FunctionDecl *decl);

  /**
   * Compile the code in the container.
   * @return True if the compilation was successful; false otherwise.
   */
  bool Compile();

  /**
   * @return True if the code in the container has been compiled; false otherwise.
   */
  bool IsCompiled() const { return module_ != nullptr; }

  void Dump();

 private:
  // The AST context used to generate the TPL ast
  ast::Context *ctx_;
  // The AST file node containing all code in the container
  ast::File *generated_file_;
  // The compiled module
  std::unique_ptr<vm::Module> module_;
};

}  // namespace tpl::sql::codegen
